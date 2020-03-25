# Common Python library imports
import difflib
from concurrent.futures import ThreadPoolExecutor as TPE
from multiprocessing import cpu_count

# Pip package imports
import pandas as pd
from loguru import logger

# Internal package imports
from miner.core import IHandler, Converter
from miner.footballdata.scrapper import FootballDataRequest

__all__ = ["FootballDataHandler", "get_default_converter"]

def get_default_converter():
    try:
        from miner.footballdata.converters import SqlConverter
        logger.debug("Class \'SqlConverter\' selected.")
        return SqlConverter
    except ImportError as err:
        logger.warning(err)
    try:
        import pandas as pd
        # TODO: Return with the fallback pandas converter
    except ImportError as err:
        logger.warning(err)
    logger.debug("Class \'Converter\' selected.")
    logger.warning("No [db_conn, pandas] packages are found. Falling back to the default Converter. Please makes sure if this is the expected behaviour")
    return Converter

class FootballDataHandler(IHandler):

    class SqlQuery(object):

        def __init__(self, *args, **kwargs):
            pass

        def get_matches_where_odds_are_null(self, start_date, end_date):
            try:
                from db_conn.connection.postgresql import ConnectionPool
                from db_conn.query.sc_soccer.select import get_matches_where_odds_are_null
            except ImportError as err:
                logger.error(err)
                return pd.DataFrame()
            else:
                pool = ConnectionPool()
                return pool.sql_query(get_matches_where_odds_are_null(start_date, end_date))


    name = "Football-Data Scrapper"
    slug = "football-data-scrapper"
    version = "v0_1"

    default_config = {
        'years': ["19/20", "18/19", "17/18", "16/17", "15/16", "14/15", "13/14", "11/12", "10/11", "09/10", "08/09", "07/08", "06/07", "05/06"],
        'alias': {
            'Wolverhampton': 'Wolves',
            'PSG': 'Paris SG',
            'Bremen': 'Werder Bremen',
            'Fortuna': 'Fortuna Dusseldorf',
            '1. FC Köln': 'FC Koln',
            'Mainz 05': 'Mainz',
            'Athletic': 'Ath Bilbao',
            'Real Sociedad': 'Sociedad',
            'ACR Messina': 'Messina',
            'Robur Siena': 'Siena',
            'Bayern M.': 'Bayern Munich',
            'Deportivo La Coruña': 'La Coruna',
        },
        'multithreading': False,
        'num_of_threads': cpu_count()
    }

    def __init__(self, *args, **kwargs):
        kwargs['config'] = { **FootballDataHandler.default_config, **kwargs.get('config', {}) }
        kwargs['converter'] = kwargs.get('converter', get_default_converter())

        m_kwargs = {**{
            'name': FootballDataHandler.name,
            'slug': FootballDataHandler.slug,
            'version': FootballDataHandler.version
        }, **kwargs}

        super(FootballDataHandler, self).__init__(*args, **m_kwargs)

        self._query_executor = kwargs.get('query', FootballDataHandler.SqlQuery())

        # Create the singleton Sofa requester
        self._req = FootballDataRequest()

    def _get_close_match(self, name, fd_name_list):
        return difflib.get_close_matches(name, fd_name_list, cutoff=0.8)
        #if len(result) == 0:
            #pass
            #logger.error("Team name: %s not found in list: %s" % (name, fd_name_list))
        #return result

    def _match_name(self, grp_data, football_df, key_var, curr_date):
        selected_match = pd.DataFrame()

        fd_home = football_df['HomeTeam'].to_list()
        fd_away = football_df['AwayTeam'].to_list()

        name_h = grp_data[key_var % 'home']
        if name_h not in self._get_config('alias').keys():
            result_h = self._get_close_match(name_h, fd_home)
        else:
            result_h = [self._get_config('alias')[name_h]]
        # 0 or more than 1 result.
        if len(result_h) != 1:
            # Match the away team names
            name_a = grp_data[key_var % 'away']
            if name_a not in self._get_config('alias').keys():
                result_a = self._get_close_match(name_a, fd_away)
            else:
                result_a = [self._get_config('alias')[name_a]]
            # If 0 result found, log error and continue
            if len(result_h) == 0 and len(result_a) == 0:
                logger.warn("At date: %s No matched name for: \'%s\' with the possibilities: %s. All possibility that day [Home]: %s | [Away]: %s" % (
                    curr_date, [name_h, name_a], (result_h + result_a), fd_home, fd_away))
            elif len(result_a) == 1:
                selected_match = football_df[(football_df['AwayTeam'] == result_a[0])]
            else:
                # Select the row, where home teams are partially match, but filter with away team correctly.
                selected_match = football_df[((football_df['AwayTeam'] == result_a[0]) & (
                    football_df['HomeTeam'].isin(result_h)))]
        else:
            # Filter with home team only
            selected_match = football_df[football_df['HomeTeam'] == result_h[0]]
        return selected_match

    def _team_name_matcher(self, data, football_df, q, curr_date):
        for row_index, grp_data in data.iterrows():
            try:
                selected_match = self._match_name(grp_data, football_df, "%s_team_short", curr_date)
                if len(selected_match) == 0:
                    selected_match = self._match_name(grp_data, football_df, "%s_team", curr_date)
                if len(selected_match) == 0:
                    continue
                q.update_match_statistic(grp_data['id'], selected_match)
                q.update_match_odds(grp_data['id'], selected_match)
            except Exception as err:
                logger.error(err)

    def _fetch_date(self, curr_date, *args, **kwargs):
        pass

    def _process(self, input_tuple):
        q = self._converter()
        tr, season, df = input_tuple
        football_df = self._req.parse_odds(tr, season)

        # Convert Date object
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        try:
            football_df['Date'] = pd.to_datetime(football_df['Date'], format='%d/%m/%Y')
        except Exception:
            football_df['Date'] = pd.to_datetime(football_df['Date'], format='%d/%m/%y')
        #football_df['Date'] = football_df['Date'].dt.date

        group_df = df.groupby(pd.Grouper(key='date', freq='1D'), group_keys=False)
        for curr_date, group_data in group_df:
            # Filter for dates
            filrtered_df = football_df[football_df['Date'] == curr_date]
            self._team_name_matcher(group_data, filrtered_df, q, curr_date)
        return q.get()

    def _do_fetch(self, start_date, end_date, *args, **kwargs):
        # Get the database query as dataframe
        query_df = self._query_executor.get_matches_where_odds_are_null(start_date, end_date)
        seasons = query_df['season'].unique()
        tournaments = query_df['tournament'].unique()

        if self._get_config('multithreading'):
            # Multithreading
            param_list = []
            # For loop one thread
            for tr in tournaments:
                # Filter for the tournament
                filtered_df = query_df[(query_df['tournament'] == tr)]
                for season in seasons:
                    # Filter for the season
                    season_filtered_df = filtered_df[ (filtered_df['season'] == season) ]
                    param_list.append( tuple( [tr, season, season_filtered_df]) )

            # player_id_gen = split_into(player_ids, cpu_count() * 5)
            with TPE(max_workers=self._get_config('num_of_threads')) as worker_pool:
                res_list = worker_pool.map(lambda x: self._process(x), param_list)
                return pd.concat(res_list)

        else:
            # For loop one thread
            res_list = []
            for tr in tournaments:
                # Filter for the tournament
                filtered_df = query_df[(query_df['tournament'] == tr)]
                for season in seasons:
                    # Filter for the season
                    filtered_df = filtered_df[ (filtered_df['season'] == season) ]
                    res_list.append(self._process( tuple([ tr, season, filtered_df ])))

            return pd.concat(res_list)
