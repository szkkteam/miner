# Common Python library imports
# Pip package imports
from loguru import logger
import pandas as pd

# Internal package imports
from miner.utils import safe_cast, intersection
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

        def update_match_odds(self, match_id, football_df):
            """
            match_id INTEGER,
            sc_odds JSON,
            fd_odds JSON
            """
            tempdict = {}
            list_of_cols = ['B365H', 'B365D', 'B365A', 'BSH', 'BSD', 'BSA', 'BWH', 'BWD', 'BWA', 'GBH', 'GBD', 'GBA', 'IWH', 'IWD', 'IWA', 'LBH', 'LBD', 'LBA', 'PSH'and'PH', 'PSD'and'PD', 'PSA'and'PA', 'SOH', 'SOD', 'SOA', 'SBH', 'SBD', 'SBA', 'SJH', 'SJD', 'SJA', 'SYH', 'SYD', 'SYA', 'VCH', 'VCD', 'VCA', 'WHH', 'WHD', 'WHA', 'Bb1X2', 'BbMxH', 'BbAvH', 'BbMxD', 'BbAvD', 'BbMxA', 'BbAvA', 'MaxH', 'MaxD', 'MaxA', 'AvgH', 'AvgD', 'AvgA', 'BbOU', 'BbMx>2.5', 'BbAv>2.5', 'BbMx<2.5', 'BbAv<2.5', 'GB>2.5', 'GB<2.5', 'B365>2.5', 'B365<2.5', 'P>2.5', 'P<2.5', 'Max>2.5', 'Max<2.5', 'Avg>2.5', 'Avg<2.5', 'BbAH', 'BbAHh', 'AHh', 'BbMxAHH', 'BbAvAHH', 'BbMxAHA', 'BbAvAHA', 'GBAHH', 'GBAHA', 'GBAH', 'LBAHH', 'LBAHA', 'LBAH', 'B365AHH', 'B365AHA', 'B365AH', 'PAHH', 'PAHA', 'MaxAHH', 'MaxAHA', 'AvgAHH', 'AvgAHA',]
            pd_cols = football_df.keys()

            try:
                filtered_df = football_df[intersection(list_of_cols, pd_cols)]
                filtered_df = filtered_df.dropna(axis='columns')
                tempdict = filtered_df.to_dict(orient='records')
                tempdict = tempdict[0]
            except Exception:
                pass

            self._q.put(str(PostgreSQLQuery.update(tables.odds
                      ).set(tables.odds.fd_odds, JSON(tempdict) if len(tempdict.keys()) > 0 else None
                      ).where(tables.odds.match_id == match_id)))

        def update_match_statistic(self, match_id, football_df):
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
            tempdict = {}
            list_of_cols = ['HS', 'AS', 'HST', 'AST', 'HHW', 'AHW', 'HC', 'AC', 'HF', 'AF', 'HFKC', 'AFKC', 'HO', 'AO', 'HY', 'AY', 'HR', 'AR', 'HBP', 'ABP', 'Time', 'HTHG', 'HTAG']
            pd_cols = football_df.keys()

            try:
                filtered_df = football_df[intersection(list_of_cols, pd_cols)]
                filtered_df = filtered_df.dropna(axis='columns')
                tempdict = filtered_df.to_dict(orient='records')
                tempdict = tempdict[0]
            except Exception:
                pass

            home_score = safe_cast(football_df.iloc[0]['FTHG'], int)
            away_score = safe_cast(football_df.iloc[0]['FTAG'], int)

            self._q.put(str(PostgreSQLQuery.update(tables.statistics
                    ).set(tables.statistics.home_score, home_score
                    ).set(tables.statistics.away_score, away_score
                    ).set(tables.statistics.fd_statistics, JSON(tempdict) if len(tempdict.keys()) > 0 else None
                    ).where(tables.statistics.match_id == match_id)))

