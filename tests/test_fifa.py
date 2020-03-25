import pandas as pd
import pytest
import miner as m
from datetime import date

test_data = [
    {'full_name': "Filippo Falco",
     'sc_player_id': 142037,
     'short_name': "F. Falco",
     'fifa_player_id': 210329,
     'birth_date': date(1992, 2, 11)},
    {'full_name': "Alisson",
     'sc_player_id': 243609,
     'short_name': "Alisson",
     'fifa_player_id': 212831,
     'birth_date': date(1992, 10, 2)},
    {'full_name': "Joachim Andersen",
     'sc_player_id': 362682,
     'short_name': "J. Andersen",
     'fifa_player_id': 224221,
     'birth_date': date(1996, 5, 31)},
]

def test_player_birthday_fetch():
    scrapper = m.fifaindex.SofaScoreScrapper()
    for data in test_data:
        sc_id, birth = scrapper.parse_player_birtdate(data['sc_player_id'])
        assert sc_id == data['sc_player_id']
        assert birth == data['birth_date']

def test_fifa_stat_fetch_by_name():
    scrapper = m.fifaindex.FifaScrapper()
    for data in test_data:
        stats, fifa_id = scrapper.parse_fifa_stats(name=data['full_name'], birth=data['birth_date'], short=data['short_name'])
        assert int(fifa_id) == data['fifa_player_id']
        assert stats is not None

def test_fifa_stat_fetch_by_fifa_id():
    scrapper = m.fifaindex.FifaScrapper()
    for data in test_data:
        stats, fifa_id = scrapper.parse_fifa_stats(fifa_idx=data['fifa_player_id'])
        assert int(fifa_id) == data['fifa_player_id']
        assert stats is not None

def test_fifa_handler_fetch_multithread():
    class CsvFeeder(m.fifaindex.FifaHandler.SqlQuery):
        def __init__(self, *args, **kwargs):
            super(CsvFeeder, self).__init__(*args, **kwargs)

        def get_null_fifa_stats(self, start_date, end_date, limit):
            if limit is not None:
                return pd.read_csv("tests/data/get_null_fifa_stats.csv", index_col=0).head(limit)
            return pd.read_csv("get_null_fifa_stats.csv", index_col=0)

        def get_all_match_for_player_id(self, player_id):
            return pd.DataFrame()

    class CustomConverter(m.core.Converter):

        def __init__(self, *args, **kwargs):
            self.store = kwargs.pop('data_store')
            super(CustomConverter, self).__init__(*args, **kwargs)

        def get(self):
            return pd.DataFrame()

        def update_player_birthday(self, player_id, birthday):
            self.store.append({'player_id': player_id, 'birthday': birthday})

        def update_fifa_id(self, player_id, fifa_id):
            self.store.append({'player_id': player_id, 'fifa_id': fifa_id})

        def update_fifa_stat(self, player_id, match_id, fifa_stat):
            self.store.append({'player_id': player_id, 'match_id': match_id, 'fifa_stat': fifa_stat})

    datastore = []
    conv = m.utils.ObjectMaker(class_=CustomConverter, data_store=datastore)
    feeder = m.utils.ObjectMaker(class_=CsvFeeder)

    handler = m.fifaindex.FifaHandler(config={'multithreading': True}, query=feeder, converter=conv)
    handler.fetch_dates(date(2019,5,5), date(219,5,6))
    assert len(datastore) > 0


def test_fifa_handler_fetch():
    class CsvFeeder(m.fifaindex.FifaHandler.SqlQuery):
        def __init__(self, *args, **kwargs):
            super(CsvFeeder, self).__init__(*args, **kwargs)

        def get_null_fifa_stats(self, start_date, end_date, limit):
            if limit is not None:
                return pd.read_csv("tests/data/get_null_fifa_stats.csv", index_col=0).head(limit)
            return pd.read_csv("get_null_fifa_stats.csv", index_col=0)

        def get_all_match_for_player_id(self, player_id):
            return pd.DataFrame()

    class CustomConverter(m.core.Converter):

        def __init__(self, *args, **kwargs):
            self.store = kwargs.pop('data_store')
            super(CustomConverter, self).__init__(*args, **kwargs)

        def get(self):
            return pd.DataFrame()

        def update_player_birthday(self, player_id, birthday):
            self.store.append({'player_id': player_id, 'birthday': birthday})

        def update_fifa_id(self, player_id, fifa_id):
            self.store.append({'player_id': player_id, 'fifa_id': fifa_id})

        def update_fifa_stat(self, player_id, match_id, fifa_stat):
            self.store.append({'player_id': player_id, 'match_id': match_id, 'fifa_stat': fifa_stat})

    datastore = []
    conv = m.utils.ObjectMaker(class_=CustomConverter, data_store=datastore)
    feeder = m.utils.ObjectMaker(class_=CsvFeeder)

    handler = m.fifaindex.FifaHandler(config={'multithreading': False}, query=feeder, converter=conv)
    handler.fetch_dates(date(2019,5,5), date(219,5,6))
    assert len(datastore) > 0
