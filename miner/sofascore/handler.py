# Common Python library imports
import traceback
from concurrent.futures import ThreadPoolExecutor as TPE
from multiprocessing import cpu_count

# Pip package imports
import pandas as pd
from loguru import logger

# Internal package imports
from miner.sofascore.scrapper import SofaRequests
from miner.core import IHandler, Converter
from miner.utils import get_nested, date_interval, listify

__all__ = ["SofaHandler", "get_default_converter"]

def get_default_converter():
    try:
        from miner.sofascore.converters import SqlConverter
        logger.debug("Class \'SqlConverter\' selected.")
        return SqlConverter
    except ImportError as err:
        logger.warning(err)
    try:
        import pandas as pd
        logger.debug("Class \'DfConverter\' selected.")
        from miner.sofascore.converters import DfConverter
        return DfConverter
    except ImportError as err:
        logger.warning(err)
    logger.debug("Class \'Converter\' selected.")
    logger.warning("No [db_conn, pandas] packages are found. Falling back to the default Converter. Please makes sure if this is the expected behaviour")
    return Converter

class SofaHandler(IHandler):


    name = "Sofascore scrapper"
    slug = "sofascore-scrapper"
    version = "v0_1"

    default_config = {
        'headers': {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0'
        },
        'tournaments': {
            "premier-league": 17,
            "laliga": 8,
            "bundesliga": 35,
            "serie-a": 23,
            "ligue-1": 34
        },
        'multithreading': False,
        'num_of_threads': cpu_count()
    }


    def __init__(self, *args, **kwargs):
        kwargs['config'] = { **SofaHandler.default_config, **kwargs.get('config', {}) }
        kwargs['converter'] = kwargs.get('converter', get_default_converter())

        m_kwargs = {**{
            'name': SofaHandler.name,
            'slug': SofaHandler.slug,
            'version': SofaHandler.version
        }, **kwargs}

        super(SofaHandler, self).__init__(*args, **m_kwargs)

        # Create the singleton Sofa requester
        self._req = SofaRequests(headers=self._get_config('headers'))

    def fetch_matches(self, event_ids, **kwargs):
        event_ids = listify(event_ids)
        q = kwargs.get('converter', self._converter())
        try:
            # logger.info("Tournament: \'%s\' has %s number of events" % (tr_name, len(event_ids)))
            event_info = map(lambda x: self._req.parse_event(x), event_ids)
            lineups_info = map(lambda x: self._req.parse_lineups_event(x), event_ids)
            player_ids = list()
            try:
                for event, lineup in zip(event_info, lineups_info):
                    # Update the tournamens and season database
                    q.convert_tournaments(event['event'])
                    q.convert_season(event['event']['season'])

                    # Update the teams database
                    q.convert_teams(event['event']['homeTeam'])
                    q.convert_teams(event['event']['awayTeam'])

                    try:
                        home = [h['player'] for h in lineup['homeTeam']['lineupsSorted']]
                        away = [a['player'] for a in lineup['awayTeam']['lineupsSorted']]
                    except KeyError:
                        continue

                    # Convert stadium
                    q.convert_stadium_ref(event)
                    # Convert the referee data
                    q.convert_referee(event)
                    # Convert the match event
                    q.convert_match(event, get_nested(event, 'event', 'tournament', 'uniqueId'))
                    # Get the odds data
                    odds_json = self._req.parse_match_odds(get_nested(event, 'event', 'id'))
                    # Convert the odds
                    q.convert_match_odds(get_nested(event, 'event', 'id'), odds_json)
                    # Convert match statistics
                    q.convert_match_statistic(event)
                    # Convert players
                    players = home + away
                    for pl in players:
                        # Convert the player references
                        q.convert_player_ref(pl)
                        player_ids.append((event['event']['id'], pl['id']))
                    # Convert team lineup
                    try:
                        match_id = event['event']['id']
                        home_id = event['event']['homeTeam']['id']
                        away_id = event['event']['awayTeam']['id']
                        home_lineup = lineup['homeTeam']
                        away_lineup = lineup['awayTeam']

                        team_lineup = zip([home_id, away_id], [home_lineup, away_lineup])

                        for team_id, team_lineup in team_lineup:
                            # Convert manager
                            q.convert_manager(team_lineup)
                            # Convert team lineup
                            q.convert_team_lineup(match_id, team_id, team_lineup)
                            try:
                                for lineup_element in team_lineup['lineupsSorted']:
                                    # Convert the player lineups
                                    q.convert_player_lineup(match_id, team_id, lineup_element)
                            except KeyError:
                                continue

                    except KeyError:
                        pass

            except Exception as err:
                tb = traceback.format_exc()
                logger.error(tb)

            # logger.info("Tournament: \'%s\' has %s number of players" % (tr_name, len(player_ids)))

            # player_id_gen = split_into(player_ids, cpu_count() * 5)
            with TPE() as worker_pool:
                # player_stats_getter = create_worker(SofaScore.parse_player_stat)
                player_stats = worker_pool.map(lambda x: self._req.parse_player_stat(x), player_ids)

            for player in player_stats:
                try:
                    match_id = player['eventData']['id']
                    player_id = player['player']['id']
                except (KeyError, TypeError) as err:
                    continue

                # Convert the player statistics
                q.convert_player_stats(match_id, player_id, player)

        except Exception as err:
            tb = traceback.format_exc()
            logger.error(tb)
            # continue
        finally:
            return q.get()


    def _get_tournaments(self, date):
        tr_list = []
        try:
            day_events = self._req.parse_by_date(date)
            tournaments = day_events['sportItem']['tournaments']
        except Exception as err:
            logger.error("Error occured when tried to parse by date. \'%s\'" % err)
        else:
            for tr in tournaments:
                # If tournaments is not in the filtered list, continue
                if not self._filter_tournament_id(get_nested(tr, 'tournament', 'uniqueId')):
                    continue
                else:
                    tr_list.append(tr)
        return tr_list

    def _filter_tournament_id(self, id):
        tournaments_ids = set(self._get_config('tournaments').values())
        res = id in tournaments_ids
        return res

    def _fetch_tournament(self, tr, *args, **kwargs):
        tr_name = get_nested(tr, 'tournament', 'name', default="Unknown")
        curr_date = kwargs.get('date', "")

        q = self._converter(name=tr_name + '-' + str(curr_date))
        try:
            events = tr['events']
            event_ids = list()
            for event in events:
                try:
                    event_ids.append(event['id'])
                except KeyError:
                    continue

            return self.fetch_matches(event_ids, converter=q)
        except Exception as err:
            tb = traceback.format_exc()
            logger.error(tb)
            return pd.DataFrame(), pd.DataFrame()
            # continue

    def _fetch_date(self, curr_date, *args, **kwargs):
        tournaments = self._get_tournaments(curr_date)

        self.info("Fetching %s tournament from date %s" %  (len(tournaments), curr_date))
        if self._get_config('multithreading'):
            threads = self._get_config('num_of_threads')
            with TPE(max_workers=threads) as pool:
                lst = pool.map(lambda x: self._fetch_tournament(x, date=curr_date, *args, **kwargs), tournaments)
            matches, player_stats = list(map(list, zip(*lst)))            
            return pd.concat(matches), pd.concat(player_stats)

        else:
            lst = list(map(lambda x: self._fetch_tournament(x, date=curr_date, *args, **kwargs), tournaments))
            matches, player_stats = list(map(list, zip(*lst)))
            return pd.concat(matches), pd.concat(player_stats)


    def _do_fetch(self, start_date, end_date, *args, **kwargs):
        lst = []
        for curr_date in date_interval(start_date, end_date):
            lst.append(self._fetch_date(curr_date, **kwargs))
        matches, player_stats = list(map(list, zip(*lst)))
        return pd.concat(matches), pd.concat(player_stats)