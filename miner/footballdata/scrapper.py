# Common Python library imports
from io import StringIO

# Pip package imports
from loguru import logger
import requests
import pandas as pd
from requests.exceptions import Timeout, HTTPError

# Internal package imports
from miner.utils import Singleton, retry

__all__ = ["FootballDataRequest"]

class FootballDataRequest(metaclass=Singleton):

    urls = {
        "premier-league" : "http://www.football-data.co.uk/mmz4281/{year}/E0.csv",
        "laliga" : "http://www.football-data.co.uk/mmz4281/{year}/SP1.csv",
        "bundesliga" : "http://www.football-data.co.uk/mmz4281/{year}/D1.csv",
        "serie-a" : "http://www.football-data.co.uk/mmz4281/{year}/I1.csv",
        "ligue-1" : "http://www.football-data.co.uk/mmz4281/{year}/F1.csv"}

    def __init__(self):
        pass

    def _convert_year(self, sofa_year):
        return sofa_year.replace('/', '')

    @retry(Timeout, tries=4, delay=2)
    def get(irl, url):
        logger.info("Opening URL: \'%s\'." % url)
        try:
            response = requests.get(url, timeout=(3,6))
            response.raise_for_status()
        except HTTPError as err:
            return None
        return pd.read_csv(StringIO(response.text))

    def parse_odds(self, tournament, year):
        url = FootballDataRequest.urls[tournament].format(year=self._convert_year(year))
        return self.get(url)