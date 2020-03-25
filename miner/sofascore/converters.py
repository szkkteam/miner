# Common Python library imports
import re
from datetime import datetime

# Pip package imports
from loguru import logger
import pandas as pd

# Internal package imports
from miner.utils import get_nested, safe_cast, listify
from miner.core import Converter

try:
    # Pip package imports
    from pypika import Query, Table, Field, enums, JSON
    from pypika.dialects import PostgreSQLQuery

    # Internal package imports
    from db_conn.query.sc_soccer import tables
    from db_conn.queue import InsertQueue
    from db_conn.connection.postgresql import ConnectionPool

except ImportError as err:
    logger.warning(err)
else:
    class SqlConverter(Converter):

        def __init__(self, *args, **kwargs):
            conn_config = kwargs.pop('config', {})
            conn = kwargs.pop('connection', ConnectionPool(config=conn_config))
            self._q = kwargs.get('queue', InsertQueue(pool=conn, **kwargs))
            super(SqlConverter, self).__init__(*args, **kwargs)

        def get(self):
            return self._q.fire_workers()

        def convert_tournaments(self, tr):
            """
            tournament_id INTEGER PRIMARY KEY,
            tournament_name VARCHAR(100),
            tournament_short VARCHAR(50)
            """
            id = safe_cast(get_nested(tr, 'tournament', 'uniqueId'), int)
            name = get_nested(tr,'tournament', 'name')
            slug = get_nested(tr, 'tournament', 'slug')
            # Create query
            self._q.put(str(Query.into(tables.tournaments).insert(
                id,
                name,
                slug)))

        def convert_season(self, season):
            """
            season_id INTEGER PRIMARY KEY,
            season_year VARCHAR(20),
            season_name VARCHAR(50),
            season_slug VARCHAR(50)
            """
            name = get_nested(season, 'name')
            id = get_nested(season, 'id')
            slug = get_nested(season, 'slug')
            year = get_nested(season, 'year')
            # Create query
            self._q.put(str(Query.into(tables.seasons).insert(
                id,
                year,
                name,
                slug)))

        def convert_teams(self, team):
            """
            team_id INTEGER PRIMARY KEY,
            team_name VARCHAR(100),
            team_slug VARCHAR(50),
            team_short VARCHAR(50)
            """
            name = get_nested(team, 'name')
            id = get_nested(team, 'id')
            slug = get_nested(team, 'slug')
            short = get_nested(team, 'shortName')
            # Create query
            self._q.put(str(Query.into(tables.teams).insert(
                id,
                name,
                slug,
                short)))


        def convert_match(self, event_info, tr_id):
            """
            match_id INTEGER PRIMARY KEY,
            season_id INTEGER,
            match_date DATE,
            full_date TIMESTAMP,
            match_status VARCHAR(50),
            home_team_id INTEGER,
            away_team_id INTEGER,
            referee_id INTEGER,
            stadium_id INTEGER,
            """
            id = get_nested(event_info, 'event', 'id')
            season_id = get_nested(event_info, 'event', 'season', 'id')
            formatted_date = get_nested(event_info, 'event', 'formatedStartDate')
            formatted_full_date = get_nested(event_info, 'event', 'startTime')
            try:
                concat_str = formatted_date + ' ' + formatted_full_date
                formatted_date = datetime.strptime((formatted_date), '%d.%m.%Y.')
                #formatted_date = "%d-%02d-%02d" % (formatted_date.year, formatted_date.month, formatted_date.day)

                formatted_full_date = datetime.strptime((concat_str), '%d.%m.%Y. %H:%M')
                #formatted_full_date = "%d-%02d-%02d %02d:%02d" % (formatted_full_date.year, formatted_full_date.month, formatted_full_date.day, formatted_full_date.hour, formatted_full_date.minute)
            except Exception as err:
                formatted_full_date = None
                formatted_date = None

            try:
                import pytz
                from pytz import timezone

                # This code snippet is taking the Sofascore Time informations which is defined in GMT+0
                gmt0 = timezone('Etc/GMT+0')
                gmt0_time = gmt0.localize(formatted_full_date)
                # Getting the local timezone. (For us Budapest is enough) it is in GMT+1 or GMT+2 n the summer
                local_tz = timezone('Europe/Budapest')
                # Convert the Sofascore time infromation from GMT+0 to GMT+2
                formatted_full_date = gmt0_time.astimezone(local_tz)

            except ImportError as err:
                logger.error("Please install pytz like: pip install pytz")

            status = get_nested(event_info, 'event', 'status', 'type')
            home_id = get_nested(event_info, 'event', 'homeTeam', 'id')
            away_id = get_nested(event_info, 'event', 'awayTeam', 'id')
            referre_id = get_nested(event_info, 'event', 'referee', 'id')
            stadium_id = get_nested(event_info, 'event', 'venue', 'id')
            # Create query
            self._q.put(str(PostgreSQLQuery.into(tables.matches).insert(
                id,
                tr_id,
                season_id,
                formatted_date,
                formatted_full_date,
                status,
                home_id,
                away_id,
                referre_id,
                stadium_id)))


        def convert_referee(self, event_info):
            """
            referee_id INTEGER PRIMARY KEY,
            referee_name VARCHAR(50),
            yellow_card_per_game FLOAT,
            red_card_per_game FLOAT
            """
            referre_id = get_nested(event_info, 'event', 'referee', 'id')
            yellow_card = get_nested(event_info, 'event', 'referee', 'yellowCardsPerGame')
            red_card = get_nested(event_info, 'event', 'referee', 'redCardsPerGame')
            yellow_card = safe_cast(yellow_card, float)
            red_card = safe_cast(red_card, float)
            referre_name = get_nested(event_info, 'event', 'referee', 'name')
            # Create query
            self._q.put(str(Query.into(tables.referees).insert(
                referre_id,
                referre_name,
                yellow_card,
                red_card)))


        def convert_match_odds(self, event_id, data_odds):
            """
            match_id INTEGER,
            sc_odds JSON,
            fd_odds JSON
            """
            tempdict = {}

            ord_markets = ['Full time', 'Double chance', '1st half', 'Draw no bet', 'Both teams to score', '']
            special_markets = ['Match goals', 'Asian handicap', 'First team to score']

            def replace_name(name):
                replaced = ""
                for c in name:
                    if c == '1':
                        replaced += 'home'
                    elif c == '2':
                        replaced += 'away'
                    elif c.lower() == 'x':
                        replaced += 'draw'
                    else:
                        break
                    replaced += '_'
                if len(replaced) > 0:
                    return replaced[:-1]
                else:
                    return name

            def split_odds(fractional_velue):
                try:
                    x1, x2 = fractional_velue.split("/")
                    decimal_value = (int(x1) / int(x2)) + 1
                    return decimal_value
                except Exception:
                    return 0

            odds = data_odds['markets']
            try:
                for odd in odds:
                    # First check if the market is an ordnary type.
                    if odd['marketName'] in ord_markets:
                        prefix = odd['marketName'].lower().replace(' ', '_')
                        for choice in odd['choices']:
                            tempdict['%s_%s' % (prefix, replace_name(choice['name']))] = split_odds(choice['fractionalValue'])

                    elif odd['marketName'] in special_markets:
                        if odd['marketName'] == 'Match goals':
                            prefix = odd['choiceGroup'].lower().replace('.', '_')
                            for choice in odd['choices']:
                                tempdict['%s_%s' % (prefix, choice['name'].lower())] = split_odds(choice['fractionalValue'])
                        if odd['marketName'] == 'Asian handicap':
                            prefix = odd['marketName'].lower().replace(' ', '_')
                            tempdict['%s_home' % prefix] = split_odds(odd['choices'][0]['fractionalValue'])
                            tempdict['%s_away' % prefix] = split_odds(odd['choices'][1]['fractionalValue'])
                        if odd['marketName'] == 'First team to score':
                            prefix = odd['marketName'].lower().replace(' ', '_')
                            tempdict['%s_home' % prefix] = split_odds(odd['choices'][0]['fractionalValue'])
                            tempdict['%s_away' % prefix] = split_odds(odd['choices'][1]['fractionalValue'])
                    else:
                        pass
                        # Currently not supported. Skip it
            except Exception:
                pass

            id = event_id

            # Create query
            self._q.put(str(PostgreSQLQuery.into(tables.odds).insert(
                id,
                JSON(tempdict) if len(tempdict.keys()) > 0 else None,
                None)))


        def convert_match_statistic(self, event_info):
            """
            match_id INTEGER PRIMARY KEY,
            sc_statistics JSON,
            fd_statistics JSON,
            sc_forms JSON,
            sc_votes JSON,
            sc_manager_duels JSON,
            sc_h2h JSON,
            home_score FLOAT,
            away_score FLOAT,
            """

            def parse_statistics(statistics):
                temp_dict = {}
                try:
                    for period in statistics['periods']:
                        try:
                            prefix = period['period'].lower() + '_'

                            for group in period['groups']:
                                try:
                                    for item in group['statisticsItems']:
                                        try:
                                            item_name = item['name'].lower().replace(' ', '_')
                                            temp_dict['%s_%s_home' % (prefix, item_name)] = item['home']
                                            temp_dict['%s_%s_away' % (prefix, item_name)] = item['away']
                                        except Exception:
                                            continue
                                except Exception:
                                    continue
                        except Exception:
                            continue
                except Exception:
                    pass
                return temp_dict

            def parse_teams_form(form):
                temp_dict = {}

                def parse_form(team, name):
                    temp_dict = {}
                    temp_dict['%s_avg_rating' % name] = safe_cast(team['avgRating'], float, default="")
                    temp_dict['%s_position' % name] = safe_cast(team['position'], int, default="")
                    temp_dict['%s_points' % name] = safe_cast(team['points'], float, default="")
                    for idx, form in enumerate(team['form']):
                        temp_dict['%s_form_%s' % (name, idx)] = form
                    return temp_dict
                try:
                    temp_dict = { **temp_dict, **parse_form(form['homeTeam'], 'home') }
                    temp_dict = {**temp_dict, **parse_form(form['awayTeam'], 'away')}
                except Exception:
                    pass
                return temp_dict

            def convert_vote(votes):
                dataframe_dict = {}
                # Get the votes
                try:
                    dataframe_dict['vote_home'] = get_nested(votes, 'vote1', default="")
                    dataframe_dict['vote_away'] = get_nested(votes, 'vote2', default="")
                    dataframe_dict['vote_draw'] = get_nested(votes, 'voteX', default="")
                    dataframe_dict['vote_home_perc'] = get_nested(votes, 'vote1ScaledPercentage', default="")
                    dataframe_dict['vote_away_perc'] = get_nested(votes, 'vote2ScaledPercentage', default="")
                    dataframe_dict['vote_draw_perc'] = get_nested(votes, 'voteXScaledPercentage', default="")
                except Exception:
                    pass

                return dataframe_dict

            def convert_managerduel(manager_duels):
                dataframe_dict = {}
                try:
                    # Get manager duels
                    dataframe_dict['manager_home_win'] = get_nested(manager_duels, 'homeManagerWins', default="")
                    dataframe_dict['manager_away_win'] = get_nested(manager_duels, 'awayManagerWins', default="")
                    dataframe_dict['manager_home'] = get_nested(manager_duels, 'homeManager', 'id', default="")
                    dataframe_dict['manager_away'] = get_nested(manager_duels, 'awayManager', 'id', default="")
                except Exception:
                    pass

                return dataframe_dict

            def convert_h2hduels(h2h_duels):
                dataframe_dict = {}
                try:
                    # Get h2h
                    dataframe_dict['h2h_home'] = get_nested(h2h_duels, 'homewins', default="")
                    dataframe_dict['h2h_away'] = get_nested(h2h_duels, 'awaywins', default="")
                    dataframe_dict['h2h_draw'] = get_nested(h2h_duels, 'draws', default="")
                except Exception:
                    pass

                return dataframe_dict


            id = get_nested(event_info, 'event', 'id')
            home_score = get_nested(event_info, 'event', 'homeScore', 'current', default="")
            away_score = get_nested(event_info, 'event', 'awayScore', 'current', default="")
            home_score = safe_cast(home_score, float)
            away_score = safe_cast(away_score, float)
            # Parse the match statistics
            statistics = parse_statistics(event_info['statistics'])
            # Parse the team form
            form = parse_teams_form(event_info['teamsForm'])
            # Update votes
            votes = convert_vote(event_info['vote'])
            # Update manager duels
            manager_duels = convert_managerduel(event_info['managerDuel'])
            # Update the h2h duels
            h2h_duels = convert_h2hduels(event_info['h2hDuel'])


            # Create query
            self._q.put(str(PostgreSQLQuery.into(tables.statistics).insert(
                id,
                JSON(statistics) if len(statistics.keys()) > 0 else None,
                None,
                JSON(form) if len(form.keys()) > 0 else None,
                JSON(votes) if len(votes.keys()) > 0 else None,
                JSON(manager_duels) if len(manager_duels.keys()) > 0 else None,
                JSON(h2h_duels) if len(h2h_duels.keys()) > 0 else None,
                home_score,
                away_score)))


        def convert_team_lineup(self, match_id, team_id, lineup_info):
            """
            match_id INTEGER,
            team_id INTEGER,
            formation TEXT [],
            manager_id INTEGER,
            """
            formation = get_nested(lineup_info, 'formation')
            manager = get_nested(lineup_info, 'manager', 'id')

            # Create query
            self._q.put(str(PostgreSQLQuery.into(tables.lineups).insert(
                match_id,
                team_id,
                formation if len(formation) > 0 else None,
                manager)))


        def convert_manager(self, lineup_info):
            """
            manager_id INTEGER PRIMARY KEY,
            manager_name VARCHAR(50)
            """
            manager_id = get_nested(lineup_info, 'manager', 'id')
            manager_name = get_nested(lineup_info, 'manager', 'name')

            # Create query
            self._q.put(str(Query.into(tables.managers).insert(
                manager_id,
                manager_name)))


        def convert_player_lineup(self, match_id, team_id, lineup_info):
            """
            match_id INTEGER,
            team_id INTEGER,
            sc_player_id INTEGER,
            player_position_long VARCHAR(50),
            player_position_short VARCHAR(20),
            sc_rating FLOAT,
            substitute BOOLEAN,
            """
            player_id = get_nested(lineup_info, 'player', 'id')
            position_name = get_nested(lineup_info, 'positionName')
            position_short = get_nested(lineup_info, 'positionNameshort')
            substitute = get_nested(lineup_info, 'substitute')
            rating = get_nested(lineup_info, 'rating')
            rating = safe_cast(rating, float)

            # Create query
            self._q.put(str(Query.into(tables.player_lineups).insert(
                match_id,
                team_id,
                player_id,
                position_name,
                position_short,
                rating,
                substitute)))


        def convert_player_ref(self, player_info):
            """
            sc_player_id INTEGER PRIMARY KEY,
            fifa_player_id INTEGER,
            full_name VARCHAR(100),
            slug VARCHAR(50),
            short_name VARCHAR(50),
            birth_date DATE,
            height FLOAT
            """
            name = get_nested(player_info, 'name')
            id = get_nested(player_info, 'id')
            slug = get_nested(player_info, 'slug')
            short = get_nested(player_info, 'shortName')

            # Create query
            self._q.put(str(Query.into(tables.players).insert(
                id,
                None,
                name,
                slug,
                short,
                None,
                None)))


        def convert_player_stats(self, match_id, player_id, stat_info):
            """
            sc_player_id integer,
            match_id integer,
            sc_stat blob,
            fifa_stat blob,
            has_sc_stat boolean,
            has_fifa_stat boolean,
            PRIMARY KEY (sc_player_id, match_id),
            FOREIGN KEY (match_id) REFERENCES Matches,
            FOREIGN KEY (sc_player_id) REFERENCES Players_ref
            """
            def parse_value(d, keys):
                for key in listify(keys):
                    name_val_pair = d.get(key)
                    if name_val_pair is None:
                        continue
                    if 'raw' in name_val_pair.keys():
                        return safe_cast(name_val_pair['raw'], int)
                    name, val = name_val_pair.values()
                    val = re.findall(r'\d+', val)
                    if len(val) == 0:
                        continue
                    return safe_cast(val[0], int)
                return ""

            def parse_statistics(stat_dict):
                # normal player
                stats = {}
                try:
                    gr = stat_dict['groups']

                    # Summary
                    stats['goalAssist'] = parse_value(get_nested(gr, 'summary', 'items'), 'goalAssist')
                    stats['goals'] = parse_value(get_nested(gr, 'summary', 'items'), 'goals')
                    stats['minutesPlayed'] = parse_value(get_nested(gr, 'summary', 'items'), 'minutesPlayed')
                    # Attack
                    stats['shotsBlocked'] = parse_value(get_nested(gr, 'attack', 'items'), 'shotsBlocked')
                    stats['shotsOffTarget'] = parse_value(get_nested(gr, 'attack', 'items'), 'shotsOffTarget')
                    stats['shotsOnTarget'] = parse_value(get_nested(gr, 'attack', 'items'), 'shotsOnTarget')
                    stats['totalContest'] = parse_value(get_nested(gr, 'attack', 'items'), 'totalContest')
                    # Defence
                    stats['challengeLost'] = parse_value(get_nested(gr, 'defence', 'items'), 'challengeLost')
                    stats['interceptionWon'] = parse_value(get_nested(gr, 'defence', 'items'), ['interceptionWon', 'interceptionWin'])
                    stats['outfielderBlock'] = parse_value(get_nested(gr, 'defence', 'items'), 'outfielderBlock')
                    stats['totalClearance'] = parse_value(get_nested(gr, 'defence', 'items'), 'totalClearance')
                    stats['totalTackle'] = parse_value(get_nested(gr, 'defence', 'items'), ['wonTackel', 'totalTackle'])
                    # Duels
                    stats['dispossessed'] = parse_value(get_nested(gr, 'duels', 'items'), 'dispossessed')
                    stats['fouls'] = parse_value(get_nested(gr, 'duels', 'items'), 'fouls')
                    stats['totalDuels'] = parse_value(get_nested(gr, 'duels', 'items'), ['totalDuels', 'groundDuels'])
                    stats['wasFouled'] = parse_value(get_nested(gr, 'duels', 'items'), 'wasFouled')
                    # Passing
                    stats['accuratePass'] = parse_value(get_nested(gr, 'passing', 'items'), 'accuratePass')
                    stats['keyPass'] = parse_value(get_nested(gr, 'passing', 'items'), 'keyPass')
                    stats['totalCross'] = parse_value(get_nested(gr, 'passing', 'items'), 'totalCross')
                    stats['totalLongBalls'] = parse_value(get_nested(gr, 'passing', 'items'), 'totalLongBalls')

                    if len(gr) == 6:
                        # goalkeeper
                        stats['goodHighClaim'] = parse_value(get_nested(gr, 'goalkeeper', 'items'), 'goodHighClaim')
                        stats['punches'] = parse_value(get_nested(gr, 'goalkeeper', 'items'), 'punches')
                        stats['runsOut'] = parse_value(get_nested(gr, 'goalkeeper', 'items'), 'runsOut')
                        stats['saves'] = parse_value(get_nested(gr, 'goalkeeper', 'items'), 'saves')
                except KeyError:
                    pass

                return stats

            stat = parse_statistics(stat_info)

            has_sc_stat = True if stat is not None else False
            # Create query
            self._q.put(str(Query.into(tables.players_stats).insert(
                player_id,
                match_id,
                JSON(stat) if len(stat.keys()) > 0 else None,
                None,
                has_sc_stat,
                True)))

        def convert_stadium_ref(self, event_info):
            """
            stadium_id INTEGER PRIMARY KEY,
            country VARCHAR(50),
            city VARCHAR(50),
            name VARCHAR(50),
            capacity INTEGER
            """
            id = get_nested(event_info, 'event', 'venue', 'id')
            country = get_nested(event_info, 'event', 'venue', 'country', 'name')
            city = get_nested(event_info, 'event', 'venue', 'city', 'name')
            name = get_nested(event_info, 'event', 'venue', 'stadium', 'name')
            capcity = get_nested(event_info, 'event', 'venue', 'stadium', 'capacity')
            capcity = safe_cast(capcity, int)

            # Create query
            self._q.put(str(Query.into(tables.stadiums).insert(
                id,
                country,
                city,
                name,
                capcity)))


class DfConverter(Converter):

    def __init__(self, *args, **kwargs):
        # Define all the tables
        self._tournaments_df = pd.DataFrame()
        self._teams_df = pd.DataFrame()
        self._seasons_df = pd.DataFrame()
        self._matches_df = pd.DataFrame()
        self._referees_df = pd.DataFrame()
        self._odds_df = pd.DataFrame()
        self._match_stats_df = pd.DataFrame()
        self._team_lineups_df = pd.DataFrame()
        self._managers_df = pd.DataFrame()
        self._player_lineups_df = pd.DataFrame()
        self._players_df = pd.DataFrame()
        self._player_stats_df = pd.DataFrame()
        self._stadiums_df = pd.DataFrame()

        super(DfConverter, self).__init__(*args, **kwargs)

    def get(self):

        def join_player_lineup(lineups, matches):
            df_list = []

            match_grp = lineups.groupby('match_id')
            for match_id, e_match_grp in match_grp:
                # TODO: Remove the grouped column
                fixed_e_match_grp = e_match_grp.drop('match_id', axis=1)
                new_cols = []
                # Group by teams also
                team_grp = fixed_e_match_grp.groupby('team_id')
                for team_id, e_team_grp in team_grp:
                    # TODO: Remove the grouped column
                    fixed_e_team_grp = e_team_grp.drop('team_id', axis=1)
                    matches_filtered = matches[matches['match_id'] == match_id]
                    if matches_filtered['home_team_id'].iloc[0] == team_id:
                        prefix = 'home'
                    else:
                        prefix = 'away'
                    rows = len(fixed_e_team_grp.index)
                    cols = fixed_e_team_grp.keys()
                    for i in range(0, rows):
                        for col in cols:
                            new_cols.append(prefix + '_' + col + '_' + str(i))

                values_df = fixed_e_match_grp.drop('team_id', axis=1)
                values_list = values_df.values.flatten()
                new_df = pd.DataFrame([values_list], columns=new_cols)
                new_df['match_id'] = match_id
                df_list.append(new_df)

            return pd.concat(df_list)

        joined_df = self._matches_df
        joined_df = pd.merge(joined_df, self._tournaments_df, how='left', left_on='tournament_id',
                             right_on='tournament_id', copy=False)

        joined_df = pd.merge(joined_df, self._seasons_df, how='left', left_on='season_id',
                             right_on='season_id', copy=False)

        joined_df = pd.merge(joined_df, self._teams_df, how='left', left_on='home_team_id',
                             right_on='team_id')
        joined_df = joined_df.rename(columns={'team_name': 'home_team_name', 'team_slug': 'home_team_slug', 'team_short': 'home_team_short'})

        joined_df = pd.merge(joined_df, self._teams_df, how='left', left_on='away_team_id',
                             right_on='team_id')
        joined_df = joined_df.rename(columns={'team_name': 'away_team_name', 'team_slug': 'away_team_slug', 'team_short': 'away_team_short'})

        joined_df = pd.merge(joined_df, self._referees_df, how='left', left_on='referee_id',
                             right_on='referee_id', copy=False)

        joined_df = pd.merge(joined_df, self._odds_df, how='left', left_on='match_id',
                             right_on='match_id', copy=False)

        joined_df = pd.merge(joined_df, self._match_stats_df, how='left', left_on='match_id',
                             right_on='match_id', copy=False)

        home_lineup = pd.merge(self._team_lineups_df, self._managers_df, how='left', left_on='manager_id',
                             right_on='manager_id')
        home_lineup = home_lineup.rename(columns={'formation': 'home_formation', 'manager_id': 'home_manager_id', 'manager_name': 'home_manager_name'})

        away_lineup = pd.merge(self._team_lineups_df, self._managers_df, how='left', left_on='manager_id',
                             right_on='manager_id')
        away_lineup = away_lineup.rename(columns={'formation': 'away_formation', 'manager_id': 'away_manager_id', 'manager_name': 'away_manager_name'})

        joined_df = pd.merge(joined_df, home_lineup, how='left', left_on=['match_id', 'home_team_id'],
                             right_on=['match_id', 'team_id'])
        joined_df = joined_df.rename(columns={'formation': 'home_formation'})

        joined_df = pd.merge(joined_df, away_lineup, how='left', left_on=['match_id', 'away_team_id'],
                             right_on=['match_id', 'team_id'])
        joined_df = joined_df.rename(columns={'formation': 'away_formation'})

        joined_df = pd.merge(joined_df, self._stadiums_df, how='left', left_on='stadium_id',
                             right_on='stadium_id', copy=False)

        flattened_lineups = join_player_lineup(self._player_lineups_df, self._matches_df)

        joined_df = pd.merge(joined_df, flattened_lineups, how='left', left_on='match_id',
                             right_on='match_id', copy=False)

        return joined_df, self._player_stats_df

    def convert_tournaments(self, tr):
        """
        tournament_id INTEGER PRIMARY KEY,
        tournament_name VARCHAR(100),
        tournament_short VARCHAR(50)
        """
        temp = {}
        temp['tournament_id'] = safe_cast(get_nested(tr, 'tournament', 'uniqueId'), int)
        temp['tournament_name'] = get_nested(tr,'tournament', 'name')
        temp['tournament_short'] = get_nested(tr, 'tournament', 'slug')
        self._tournaments_df = self._tournaments_df.append(temp, ignore_index=True)

    def convert_season(self, season):
        """
        season_id INTEGER PRIMARY KEY,
        season_year VARCHAR(20),
        season_name VARCHAR(50),
        season_slug VARCHAR(50)
        """
        temp = {}
        temp['season_id'] = safe_cast(get_nested(season, 'id'), int)
        temp['season_year'] = get_nested(season, 'year')
        temp['season_name'] = get_nested(season, 'name')
        temp['season_slug'] = get_nested(season, 'slug')
        self._seasons_df = self._seasons_df.append(temp, ignore_index=True)

    def convert_teams(self, team):
        """
        team_id INTEGER PRIMARY KEY,
        team_name VARCHAR(100),
        team_slug VARCHAR(50),
        team_short VARCHAR(50)
        """
        temp = {}
        temp['team_id'] = safe_cast(get_nested(team, 'id'), int)
        temp['team_name'] = get_nested(team, 'name')
        temp['team_slug'] = get_nested(team, 'slug')
        temp['team_short'] = get_nested(team, 'shortName')
        self._teams_df = self._teams_df.append(temp, ignore_index=True)

    def convert_match(self, event_info, tr_id):
        """
        match_id INTEGER PRIMARY KEY,
        season_id INTEGER,
        match_date DATE,
        full_date TIMESTAMP,
        match_status VARCHAR(50),
        home_team_id INTEGER,
        away_team_id INTEGER,
        referee_id INTEGER,
        stadium_id INTEGER,
        """
        temp = {}
        temp['match_id'] = safe_cast(get_nested(event_info, 'event', 'id'), int)
        temp['season_id'] = safe_cast(get_nested(event_info, 'event', 'season', 'id'), int)

        formatted_date = get_nested(event_info, 'event', 'formatedStartDate')
        formatted_full_date = get_nested(event_info, 'event', 'startTime')
        try:
            concat_str = formatted_date + ' ' + formatted_full_date
            formatted_date = datetime.strptime((formatted_date), '%d.%m.%Y.')
            #formatted_date = "%d-%02d-%02d" % (formatted_date.year, formatted_date.month, formatted_date.day)

            formatted_full_date = datetime.strptime((concat_str), '%d.%m.%Y. %H:%M')
            #formatted_full_date = "%d-%02d-%02d %02d:%02d" % (formatted_full_date.year, formatted_full_date.month, formatted_full_date.day, formatted_full_date.hour, formatted_full_date.minute)
        except Exception as err:
            formatted_full_date = None
            formatted_date = None

        try:
            import pytz
            from pytz import timezone

            # This code snippet is taking the Sofascore Time informations which is defined in GMT+0
            gmt0 = timezone('Etc/GMT+0')
            gmt0_time = gmt0.localize(formatted_full_date)
            # Getting the local timezone. (For us Budapest is enough) it is in GMT+1 or GMT+2 n the summer
            local_tz = timezone('Europe/Budapest')
            # Convert the Sofascore time infromation from GMT+0 to GMT+2
            formatted_full_date = gmt0_time.astimezone(local_tz)

        except ImportError as err:
            logger.error("Please install pytz like: pip install pytz")

        temp['full_date'] = formatted_full_date
        temp['match_date'] = formatted_date

        temp['tournament_id'] = safe_cast(tr_id, int)

        temp['match_status'] = get_nested(event_info, 'event', 'status', 'type')
        temp['home_team_id'] = safe_cast(get_nested(event_info, 'event', 'homeTeam', 'id'), int)
        temp['away_team_id'] = safe_cast(get_nested(event_info, 'event', 'awayTeam', 'id'), int)

        temp['referee_id'] = safe_cast(get_nested(event_info, 'event', 'referee', 'id'), int)
        temp['stadium_id'] = safe_cast(get_nested(event_info, 'event', 'venue', 'id'), int)

        self._matches_df = self._matches_df.append(temp, ignore_index=True)

    def convert_referee(self, event_info):
        """
        referee_id INTEGER PRIMARY KEY,
        referee_name VARCHAR(50),
        yellow_card_per_game FLOAT,
        red_card_per_game FLOAT
        """
        temp = {}
        temp['referee_id'] = safe_cast(get_nested(event_info, 'event', 'referee', 'id'), int)
        yellow_card = get_nested(event_info, 'event', 'referee', 'yellowCardsPerGame')
        red_card = get_nested(event_info, 'event', 'referee', 'redCardsPerGame')
        temp['yellow_card_per_game'] = safe_cast(yellow_card, float)
        temp['red_card_per_game'] = get_nested(event_info, 'event', 'referee', 'name')
        self._referees_df = self._referees_df.append(temp, ignore_index=True)

    def convert_match_odds(self, event_id, data_odds):
        """
        match_id INTEGER,
        sc_odds JSON,
        fd_odds JSON
        """
        tempdict = {}

        ord_markets = ['Full time', 'Double chance', '1st half', 'Draw no bet', 'Both teams to score', '']
        special_markets = ['Match goals', 'Asian handicap', 'First team to score']
        def replace_name(name):
            replaced = ""
            for c in name:
                if c == '1':
                    replaced += 'home'
                elif c == '2':
                    replaced += 'away'
                elif c.lower() == 'x':
                    replaced += 'draw'
                else:
                    break
                replaced += '_'
            if len(replaced) > 0:
                return replaced[:-1]
            else:
                return name

        def split_odds(fractional_velue):
            try:
                x1, x2 = fractional_velue.split("/")
                decimal_value = (int(x1) / int(x2)) + 1
                return decimal_value
            except Exception:
                return 0

        odds = data_odds['markets']
        try:
            for odd in odds:
                # First check if the market is an ordnary type.
                if odd['marketName'] in ord_markets:
                    prefix = odd['marketName'].lower().replace(' ', '_')
                    for choice in odd['choices']:
                        tempdict['%s_%s' % (prefix, replace_name(choice['name']))] = split_odds(choice['fractionalValue'])

                elif odd['marketName'] in special_markets:
                    if odd['marketName'] == 'Match goals':
                        prefix = odd['choiceGroup'].lower().replace('.', '_')
                        for choice in odd['choices']:
                            tempdict['%s_%s' % (prefix, choice['name'].lower())] = split_odds(choice['fractionalValue'])
                    if odd['marketName'] == 'Asian handicap':
                        prefix = odd['marketName'].lower().replace(' ', '_')
                        tempdict['%s_home' % prefix] = split_odds(odd['choices'][0]['fractionalValue'])
                        tempdict['%s_away' % prefix] = split_odds(odd['choices'][1]['fractionalValue'])
                    if odd['marketName'] == 'First team to score':
                        prefix = odd['marketName'].lower().replace(' ', '_')
                        tempdict['%s_home' % prefix] = split_odds(odd['choices'][0]['fractionalValue'])
                        tempdict['%s_away' % prefix] = split_odds(odd['choices'][1]['fractionalValue'])
                else:
                    pass
                    # Currently not supported. Skip it
        except Exception:
            pass

        tempdict['match_id'] = safe_cast(event_id, int)
        self._odds_df = self._odds_df.append(tempdict, ignore_index=True)

    def convert_match_statistic(self, event_info):
        """
        match_id INTEGER PRIMARY KEY,
        sc_statistics JSON,
        fd_statistics JSON,
        sc_forms JSON,
        sc_votes JSON,
        sc_manager_duels JSON,
        sc_h2h JSON,
        home_score FLOAT,
        away_score FLOAT,
        """

        def parse_statistics(statistics):
            temp_dict = {}
            try:
                for period in statistics['periods']:
                    try:
                        prefix = period['period'].lower() + '_'

                        for group in period['groups']:
                            try:
                                for item in group['statisticsItems']:
                                    try:
                                        item_name = item['name'].lower().replace(' ', '_')
                                        temp_dict['%s_%s_home' % (prefix, item_name)] = item['home']
                                        temp_dict['%s_%s_away' % (prefix, item_name)] = item['away']
                                    except Exception:
                                        continue
                            except Exception:
                                continue
                    except Exception:
                        continue
            except Exception:
                pass
            return temp_dict

        def parse_teams_form(form):
            temp_dict = {}

            def parse_form(team, name):
                temp_dict = {}
                temp_dict['%s_avg_rating' % name] = safe_cast(team['avgRating'], float, default="")
                temp_dict['%s_position' % name] = safe_cast(team['position'], int, default="")
                temp_dict['%s_points' % name] = safe_cast(team['points'], float, default="")
                for idx, form in enumerate(team['form']):
                    temp_dict['%s_form_%s' % (name, idx)] = form
                return temp_dict
            try:
                temp_dict = { **temp_dict, **parse_form(form['homeTeam'], 'home') }
                temp_dict = {**temp_dict, **parse_form(form['awayTeam'], 'away')}
            except Exception:
                pass
            return temp_dict

        def convert_vote(votes):
            dataframe_dict = {}
            # Get the votes
            try:
                dataframe_dict['vote_home'] = get_nested(votes, 'vote1', default="")
                dataframe_dict['vote_away'] = get_nested(votes, 'vote2', default="")
                dataframe_dict['vote_draw'] = get_nested(votes, 'voteX', default="")
                dataframe_dict['vote_home_perc'] = get_nested(votes, 'vote1ScaledPercentage', default="")
                dataframe_dict['vote_away_perc'] = get_nested(votes, 'vote2ScaledPercentage', default="")
                dataframe_dict['vote_draw_perc'] = get_nested(votes, 'voteXScaledPercentage', default="")
            except Exception:
                pass

            return dataframe_dict

        def convert_managerduel(manager_duels):
            dataframe_dict = {}
            try:
                # Get manager duels
                dataframe_dict['manager_home_win'] = get_nested(manager_duels, 'homeManagerWins', default="")
                dataframe_dict['manager_away_win'] = get_nested(manager_duels, 'awayManagerWins', default="")
                dataframe_dict['manager_home'] = get_nested(manager_duels, 'homeManager', 'id', default="")
                dataframe_dict['manager_away'] = get_nested(manager_duels, 'awayManager', 'id', default="")
            except Exception:
                pass

            return dataframe_dict

        def convert_h2hduels(h2h_duels):
            dataframe_dict = {}
            try:
                # Get h2h
                dataframe_dict['h2h_home'] = get_nested(h2h_duels, 'homewins', default="")
                dataframe_dict['h2h_away'] = get_nested(h2h_duels, 'awaywins', default="")
                dataframe_dict['h2h_draw'] = get_nested(h2h_duels, 'draws', default="")
            except Exception:
                pass

            return dataframe_dict

        temp = {}
        temp['match_id'] = safe_cast(get_nested(event_info, 'event', 'id'), int)
        home_score = get_nested(event_info, 'event', 'homeScore', 'current', default="")
        away_score = get_nested(event_info, 'event', 'awayScore', 'current', default="")
        home_score = safe_cast(home_score, float)
        away_score = safe_cast(away_score, float)
        # Parse the match statistics
        statistics = parse_statistics(event_info['statistics'])
        # Parse the team form
        form = parse_teams_form(event_info['teamsForm'])
        # Update votes
        votes = convert_vote(event_info['vote'])
        # Update manager duels
        manager_duels = convert_managerduel(event_info['managerDuel'])
        # Update the h2h duels
        h2h_duels = convert_h2hduels(event_info['h2hDuel'])

        temp['home_score'] = home_score
        temp['away_score'] = away_score

        temp = { **temp, **statistics }
        temp = {**temp, **form}
        temp = {**temp, **votes}
        temp = {**temp, **manager_duels}
        temp = {**temp, **h2h_duels}

        self._match_stats_df = self._match_stats_df.append(temp, ignore_index=True)

    def convert_team_lineup(self, match_id, team_id, lineup_info):
        """
        match_id INTEGER,
        team_id INTEGER,
        formation TEXT [],
        manager_id INTEGER,
        """
        temp = {}
        temp['match_id'] = safe_cast(match_id, int)
        temp['team_id'] = safe_cast(team_id, int)
        temp['formation'] = get_nested(lineup_info, 'formation')
        temp['manager_id'] = safe_cast(get_nested(lineup_info, 'manager', 'id'), int)

        self._team_lineups_df = self._team_lineups_df.append(temp, ignore_index=True)

    def convert_manager(self, lineup_info):
        """
        manager_id INTEGER PRIMARY KEY,
        manager_name VARCHAR(50)
        """
        temp = {}
        temp['manager_id'] = safe_cast(get_nested(lineup_info, 'manager', 'id'), int)
        temp['manager_name'] = get_nested(lineup_info, 'manager', 'name')

        self._managers_df = self._managers_df.append(temp, ignore_index=True)

    def convert_player_lineup(self, match_id, team_id, lineup_info):
        """
        match_id INTEGER,
        team_id INTEGER,
        sc_player_id INTEGER,
        player_position_long VARCHAR(50),
        player_position_short VARCHAR(20),
        sc_rating FLOAT,
        substitute BOOLEAN,
        """
        temp = {}
        temp['match_id'] = safe_cast(match_id, int)
        temp['team_id'] = safe_cast(team_id, int)
        temp['sc_player_id'] = safe_cast(get_nested(lineup_info, 'player', 'id'), int)
        temp['player_position_long'] = get_nested(lineup_info, 'positionName')
        temp['player_position_short'] = get_nested(lineup_info, 'positionNameshort')
        temp['substitute'] = get_nested(lineup_info, 'substitute')
        rating = get_nested(lineup_info, 'rating')
        temp['sc_rating'] = safe_cast(rating, float)

        self._player_lineups_df = self._player_lineups_df.append(temp, ignore_index=True)

    def convert_player_ref(self, player_info):
        """
        sc_player_id INTEGER PRIMARY KEY,
        fifa_player_id INTEGER,
        full_name VARCHAR(100),
        slug VARCHAR(50),
        short_name VARCHAR(50),
        birth_date DATE,
        height FLOAT
        """
        temp = {}
        temp['full_name'] = get_nested(player_info, 'name')
        temp['sc_player_id'] = safe_cast(get_nested(player_info, 'id'), int)
        temp['slug'] = get_nested(player_info, 'slug')
        temp['short_name'] = get_nested(player_info, 'shortName')

        self._players_df = self._players_df.append(temp, ignore_index=True)

    def convert_player_stats(self, match_id, player_id, stat_info):
        """
        sc_player_id integer,
        match_id integer,
        sc_stat blob,
        fifa_stat blob,
        has_sc_stat boolean,
        has_fifa_stat boolean,
        PRIMARY KEY (sc_player_id, match_id),
        FOREIGN KEY (match_id) REFERENCES Matches,
        FOREIGN KEY (sc_player_id) REFERENCES Players_ref
        """
        def parse_value(d, keys):
            for key in listify(keys):
                name_val_pair = d.get(key)
                if name_val_pair is None:
                    continue
                if 'raw' in name_val_pair.keys():
                    return safe_cast(name_val_pair['raw'], int)
                name, val = name_val_pair.values()
                val = re.findall(r'\d+', val)
                if len(val) == 0:
                    continue
                return safe_cast(val[0], int)
            return ""

        def parse_statistics(stat_dict):
            # normal player
            stats = {}
            try:
                gr = stat_dict['groups']

                # Summary
                stats['goalAssist'] = parse_value(get_nested(gr, 'summary', 'items'), 'goalAssist')
                stats['goals'] = parse_value(get_nested(gr, 'summary', 'items'), 'goals')
                stats['minutesPlayed'] = parse_value(get_nested(gr, 'summary', 'items'), 'minutesPlayed')
                # Attack
                stats['shotsBlocked'] = parse_value(get_nested(gr, 'attack', 'items'), 'shotsBlocked')
                stats['shotsOffTarget'] = parse_value(get_nested(gr, 'attack', 'items'), 'shotsOffTarget')
                stats['shotsOnTarget'] = parse_value(get_nested(gr, 'attack', 'items'), 'shotsOnTarget')
                stats['totalContest'] = parse_value(get_nested(gr, 'attack', 'items'), 'totalContest')
                # Defence
                stats['challengeLost'] = parse_value(get_nested(gr, 'defence', 'items'), 'challengeLost')
                stats['interceptionWon'] = parse_value(get_nested(gr, 'defence', 'items'), ['interceptionWon', 'interceptionWin'])
                stats['outfielderBlock'] = parse_value(get_nested(gr, 'defence', 'items'), 'outfielderBlock')
                stats['totalClearance'] = parse_value(get_nested(gr, 'defence', 'items'), 'totalClearance')
                stats['totalTackle'] = parse_value(get_nested(gr, 'defence', 'items'), ['wonTackel', 'totalTackle'])
                # Duels
                stats['dispossessed'] = parse_value(get_nested(gr, 'duels', 'items'), 'dispossessed')
                stats['fouls'] = parse_value(get_nested(gr, 'duels', 'items'), 'fouls')
                stats['totalDuels'] = parse_value(get_nested(gr, 'duels', 'items'), ['totalDuels', 'groundDuels'])
                stats['wasFouled'] = parse_value(get_nested(gr, 'duels', 'items'), 'wasFouled')
                # Passing
                stats['accuratePass'] = parse_value(get_nested(gr, 'passing', 'items'), 'accuratePass')
                stats['keyPass'] = parse_value(get_nested(gr, 'passing', 'items'), 'keyPass')
                stats['totalCross'] = parse_value(get_nested(gr, 'passing', 'items'), 'totalCross')
                stats['totalLongBalls'] = parse_value(get_nested(gr, 'passing', 'items'), 'totalLongBalls')

                if len(gr) == 6:
                    # goalkeeper
                    stats['goodHighClaim'] = parse_value(get_nested(gr, 'goalkeeper', 'items'), 'goodHighClaim')
                    stats['punches'] = parse_value(get_nested(gr, 'goalkeeper', 'items'), 'punches')
                    stats['runsOut'] = parse_value(get_nested(gr, 'goalkeeper', 'items'), 'runsOut')
                    stats['saves'] = parse_value(get_nested(gr, 'goalkeeper', 'items'), 'saves')
            except KeyError:
                pass

            return stats

        stat = parse_statistics(stat_info)

        temp = {}
        temp['sc_player_id'] = safe_cast(player_id, int)
        temp['match_id'] = match_id
        temp = { **temp, **stat }

        self._player_stats_df = self._player_stats_df.append(temp, ignore_index=True)

    def convert_stadium_ref(self, event_info):
        """
        stadium_id INTEGER PRIMARY KEY,
        country VARCHAR(50),
        city VARCHAR(50),
        name VARCHAR(50),
        capacity INTEGER
        """
        temp = {}
        temp['stadium_id'] = safe_cast(get_nested(event_info, 'event', 'venue', 'id'), int)
        temp['country'] = get_nested(event_info, 'event', 'venue', 'country', 'name')
        temp['city'] = get_nested(event_info, 'event', 'venue', 'city', 'name')
        temp['name'] = get_nested(event_info, 'event', 'venue', 'stadium', 'name')
        capcity = get_nested(event_info, 'event', 'venue', 'stadium', 'capacity')
        temp['capacity'] = safe_cast(capcity, int)

        self._stadiums_df = self._stadiums_df.append(temp, ignore_index=True)
