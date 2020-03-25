# Common Python library imports

# Pip package imports
from loguru import logger
import pandas as pd

# Internal package imports
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
            return pd.DataFrame(self._q.fire_workers())


        def update_player_birthday(self, player_id, birthday):
            """
            sc_player_id INTEGER PRIMARY KEY,
            fifa_player_id INTEGER,
            full_name VARCHAR(100),
            slug VARCHAR(50),
            short_name VARCHAR(50),
            birth_date DATE,
            height FLOAT
            """
            self._q.put(str(Query.update(tables.players
                    ).set(tables.players.birth_date, birthday
                    ).where(tables.players.sc_player_id == player_id)))

        def update_fifa_id(self, player_id, fifa_id):
            """
            sc_player_id INTEGER PRIMARY KEY,
            fifa_player_id INTEGER,
            full_name VARCHAR(100),
            slug VARCHAR(50),
            short_name VARCHAR(50),
            birth_date DATE,
            height FLOAT
            """
            self._q.put(str(Query.update(tables.players
                    ).set(tables.players.fifa_player_id, fifa_id
                    ).where(tables.players.sc_player_id == player_id)))

        def update_fifa_stat(self, player_id, match_id, fifa_stat):
            """
            sc_player_id integer,
            match_id integer,
            sc_stat blob,
            fifa_stat blob,
            has_sc_stat boolean,
            has_fifa_stat boolean,
            """
            has_fifa_stat = True if fifa_stat is not None else False
            self._q.put(str(PostgreSQLQuery.update(tables.players_stats
                    ).set(tables.players_stats.fifa_stat, JSON(fifa_stat) if fifa_stat is not None else None
                    ).set(tables.players_stats.has_fifa_stat, has_fifa_stat
                    ).where((tables.players_stats.sc_player_id == player_id) & (tables.players_stats.match_id == match_id))))

        def update_has_fifa_stat(self, player_id, value=False):
            """
            UPDATE player_stats
            SET has_fifa_stat = false
            WHERE sc_player_id = 2867;
            """
            self._q.put(str(PostgreSQLQuery.update(tables.players_stats
                   ).set(tables.players_stats.has_fifa_stat, value
                   ).where(tables.players_stats.sc_player_id == player_id)))