# Common Python library imports

import traceback
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor as TPE
from multiprocessing import cpu_count

# Pip package imports
import pandas as pd
from loguru import logger

# Internal package imports
from miner.core import IHandler, Converter
from miner.fifaindex.scrapper import FifaScrapper, SofaScoreScrapper

from miner.utils import split

__all__ = ["FifaHandler", "get_default_converter"]

def get_default_converter():
    try:
        from miner.fifaindex.converters import SqlConverter
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


class FifaHandler(IHandler):

    class SqlQuery(object):

        def __init__(self, *args, **kwargs):
            pass

        def get_null_fifa_stats(self, limit):
            try:
                from db_conn.connection.postgresql import ConnectionPool
                from db_conn.query.sc_soccer.select import get_null_fifa_stats
            except ImportError as err:
                logger.error(err)
                return pd.DataFrame()
            else:
                pool = ConnectionPool()
                return pool.sql_query(get_null_fifa_stats(limit))

        def get_all_match_for_player_id(self, player_id):
            try:
                from db_conn.connection.postgresql import ConnectionPool
                from db_conn.query.sc_soccer.select import get_all_match_for_player_id
            except ImportError as err:
                logger.error(err)
                return pd.DataFrame()
            else:
                pool = ConnectionPool()
                return pool.sql_query(get_all_match_for_player_id(player_id))

    name = "Fifa Stat Scrapper"
    slug = "fifa-stat-scrapper"
    version = "v0_1"

    default_config = {
        'tournaments': {
            "premier-league": 17,
            "laliga": 8,
            "bundesliga": 35,
            "serie-a": 23,
            "ligue-1": 34
        },
        'fifa_scrapper_config': {

        },
        'sofa_scrapper_config': {

        },
        'multithreading': False,
        'num_of_threads': cpu_count(),
        'limit': 4,
    }


    def __init__(self, *args, **kwargs):
        kwargs['config'] = { **FifaHandler.default_config, **kwargs.get('config', {}) }
        kwargs['converter'] = kwargs.get('converter', get_default_converter())

        m_kwargs = {**{
            'name': FifaHandler.name,
            'slug': FifaHandler.slug,
            'version': FifaHandler.version
        }, **kwargs}

        super(FifaHandler, self).__init__(*args, **m_kwargs)
        # Create the singleton Sofa requester
        self._fifa_req = FifaScrapper(config=self._get_config('fifa_scrapper_config'))
        self._sofa_req = SofaScoreScrapper(config=self._get_config('sofa_scrapper_config'))
        query = kwargs.get('query', FifaHandler.SqlQuery)
        self._query_executor = query()

    def _split_fetch_merge(self, drv_list, df, fnc):
        threads = self._get_config('num_of_threads')
        group_df = df.groupby('player_id', group_keys=False)
        splitted_df = split(group_df, threads)

        df_list = []
        for element in splitted_df:
            merged_element = zip(drv_list, element)
            with TPE(max_workers=threads) as pool:
                df_list.append(pool.map(lambda x: fnc(x[0], x[1][1]), merged_element))

        return pd.concat(df_list)

    def _do_fetch(self, start_date, end_date, *args, **kwargs):
        query_df = self._query_executor.get_null_fifa_stats(self._get_config('limit'))
        logger.debug("[%s] entry found with empty FiFaStat" % (len(query_df)))
        # When dataframe fetched from an SQL query, all null elements are replaced by None.
        query_df.replace([None], np.nan, inplace=True)
        # Remove the redundant player ID instances.
        # TODO: Test, or modify the query to fetch less
        #query_df.drop_duplicates(subset='player_id', inplace=True)

        # This group has already a fifa index. Has to processed 1 time
        has_fifa_id = query_df[ (query_df['fifa_id'].notnull()) ]

        # This group dosen't has fifa index, but have birthdate. Has to processed 2 times
        has_birth_date = query_df[(query_df['fifa_id'].isnull()) & (query_df['birth'].notnull())]

        # This group is the worst. Dosen't has anything. Has to processed 3 times
        has_nothing = query_df[(query_df['fifa_id'].isnull()) & (query_df['birth'].isnull())]

        try:
            # Execute in sequence
            # Step 1. Get the player birth dates
            # Create a list of unique player ID's.
            new_birth_date = self._fetch_birth_dates(has_nothing['player_id'].unique())
            # Merge the newly scrapped birth dates, together with the previous birth dates.
            has_birth_date = pd.concat([has_birth_date, new_birth_date], sort=False)
            # Filter out those players whose dosent have birth date
            #has_birth_date = has_birth_date[ has_birth_date['birth'].notnull() ]
            # Step 3. Get the fifa stat by fifa ids
            return self._fetch_fifa_stat(pd.concat([has_fifa_id, has_birth_date], sort=False))
        except Exception as err:
            tb = traceback.format_exc()
            logger.error(tb)


    def _fetch_birth_dates(self, id_list, **kwargs):
        if len(id_list) == 0:
            return pd.DataFrame([], columns=['player_id', 'birth'])

        logger.debug("[%s] entry found with empty birth date" % (len(id_list)))

        def make_fetcher(q):
            def fetch(id_list):
                #q = self._converter(name="Birthday fetcher")
                result = list(map(lambda x: self._sofa_req.parse_player_birtdate(x), id_list))
                c = list(map(lambda x: q.update_player_birthday(x[0], x[1]), result))
                # Create a dataframe from the tuples
                #q.get()
                return pd.DataFrame(result, columns=['player_id', 'birth'])
            return fetch

        q = self._converter(name="Birthday fetcher")
        fnc = make_fetcher(q)
        if self._get_config('multithreading'):
            threads = self._get_config('num_of_threads')
            splitted_id_list = split(id_list, threads)
            with TPE(max_workers=threads) as pool:
                df_list = list(pool.map(lambda x: fnc(x), splitted_id_list))
        else:
            df_list = [ fnc(id_list) ]

        return pd.concat(df_list)

    def _fetch_fifa_stat(self, df):
        # Make groups with player ids
        group_df = df.groupby('player_id', group_keys=False)

        def make_fetcher(q):

            def fetch(player_id, grp):
                #q = self._converter(name="Player ID: %s" % player_id)
                try:
                    #player_id = int(grp['player_id'].unique()[0])
                    grp.replace({pd.np.nan: None}, inplace=True)
                    try:
                        fifa_idx = int(grp['fifa_id'].unique()[0])
                    except Exception:
                        fifa_idx = None
                    birth = grp['birth'].tolist()[0]
                    player_name = grp['name'].unique()[0]
                    player_name_short = grp['short'].unique()[0]

                    logger.debug("For player ID: %s - Fetching FiFaStats" % player_id)
                    fifa_stats, fifa_idx = self._fifa_req.parse_fifa_stats(birth=birth, fifa_idx=fifa_idx, name=player_name, short=player_name_short)
                    logger.debug("For player ID: %s - [%s] FiFa stat found." % (player_id, len(fifa_stats) if fifa_stats is not None else 0))

                    if fifa_stats is not None:
                        player_matches_df = self._query_executor.get_all_match_for_player_id(player_id)
                        if len(player_matches_df.index) > 0:
                            # Make sure the date's are in descending order
                            fifa_stats.sort(reverse=True, key=lambda x: x[0])
                            # List of stats and IDs
                            queue_list = []
                            # Initial first date
                            first_date = datetime.now().date()
                            for fifa_date, fifa_stat in fifa_stats:
                                filtered_df = player_matches_df[ (player_matches_df['date'] < pd.Timestamp(first_date)) & (player_matches_df['date'] > pd.Timestamp(fifa_date))]

                                match_ids = filtered_df['match_id'].tolist()
                                first_date = fifa_date
                                if len(match_ids) > 0:
                                    queue_list.append( (player_id, fifa_stat, match_ids) )

                            for player_id, fifa_stat, match_ids in queue_list:
                                for match_id in match_ids:
                                    q.update_fifa_stat(player_id, match_id, fifa_stat)
                    else:
                        q.update_has_fifa_stat(player_id, False)
                    # Update the fifa index also
                    if fifa_idx is not None:
                        q.update_fifa_id(player_id, fifa_idx)

                except Exception as err:
                    tb = traceback.format_exc()
                    logger.error(tb)
                #return q.get()

            return fetch

        q = self._converter(name="FifaStat fetcher")
        fnc = make_fetcher(q)
        if self._get_config('multithreading'):
            threads = self._get_config('num_of_threads')
            with TPE(max_workers=threads) as pool:
                pool.map(lambda x: fnc(x[0], x[1]), group_df)
        else:
            list(map(lambda x : fnc(x[0], x[1]), group_df))
        return q.get()
        #return pd.concat(df_list)
