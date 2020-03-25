# Common Python library imports
from datetime import date

# Pip package imports
from loguru import logger
from lxml import html
import requests
from requests.exceptions import Timeout, HTTPError

# Internal package imports
from miner.utils import Singleton, retry

__all__ = ["SofaRequests"]

class SofaRequests(metaclass=Singleton):

    by_date_url = "https://www.sofascore.com/football//{date}/json"  # yyyy-mm-dd
    event_url = "https://www.sofascore.com/event/{event_id}/json"
    lineups_url = "https://www.sofascore.com/event/{event_id}/lineups/json"
    lineups_url2 = "https://www.sofascore.com/event/{event_id}/lineups/embed"
    player_statistics_rul = "https://www.sofascore.com/event/{event_id}/player/{player_id}/statistics/json"
    odds_url = "https://api.sofascore.com/api/v1/event/{event_id}/odds/1/all?_="

    def __init__(self, *args, **kwargs):
        self._headers = kwargs.get('headers', {})

    @retry(Timeout, tries=4, delay=2)
    def get(self, url):
        logger.debug("Opening URL: \'%s\'." % url)
        try:
            response = requests.get(url, headers=self._headers, timeout=(3,6))
            response.raise_for_status()
        except HTTPError as err:
            print(err)
            return None
        return response

    def parse_by_date(self, curr_date):
        assert isinstance(curr_date, date), "Date input parameter is not a datetime instance."
        curr_date = "%d-%02d-%02d" % (curr_date.year, curr_date.month, curr_date.day)
        # Generator creator
        # all events are generated btw begin_date and end_date
        url = self.by_date_url.format(date=curr_date)
        return self.get(url).json()

    def parse_event(self, event_id):
        url = self.event_url.format(event_id=event_id)
        return self.get(url).json()

    def parse_lineups_event(self, event_id):
        url = self.lineups_url.format(event_id=event_id)
        return self.get(url).json()

    def parse_lineups_event_visual(self, event_id):
        response_json = {}

        def get_position(idx, len):
            if idx == 0:
                return 'G', 'Goalkeeper'
            elif idx == 1:
                return 'D', 'Defender'
            elif idx == (len - 1):
                return 'F', 'Forward'
            else:
                return 'M', 'Midfielder'

        url = self.lineups_url2.format(event_id=event_id)
        content = self.get(url).content
        content = html.fromstring(content)
        teams = content.xpath('.//div[contains(@id, "team")]')
        for team in teams:
            side_lineup = {}
            side_lineup['lineupsSorted'] = []
            side = team.get('data-lineup-type')
            cols = team.xpath('.//div[@class="cell cell--vertical u-h420"]')
            position = 1
            formation = []
            for idx, col in enumerate(cols):
                rows = col.xpath('.//div[@class="cell__section lineups__player"]')
                formation.append(len(rows))
                for row in rows:
                    name = row.get('data-player-name')
                    id = row.get('data-id')

                    pos_short, pos_long = get_position(idx, len(cols))
                    side_lineup['lineupsSorted'].append( {
                        'position' : position,
                        'substitute' : False,
                        'positionName': pos_long,
                        'positionNameshort': pos_short,
                        'rating': None,
                        'captain': "",
                        'player': {
                            'name': name,
                            'slug': name.lower().replace(' ', '-'),
                            'shortName': "",
                            'id': id,
                        }
                    })
                    position += 1
            side_lineup['formation'] = formation[1:]
            side_lineup['hasLineups'] = True
            side_lineup['confirmedLineups'] = True
            side_lineup['hasSubstitutes'] = False
            side_lineup['hasSubstitutes'] = []
            side_lineup['incidents'] = {}
            side_lineup['manager'] = {
                'id': None,
                'Name': "",
                'slug': ''
            }


            if side == 'home':
                response_json['homeTeam'] = side_lineup
            else:
                response_json['awayTeam'] = side_lineup
        return response_json

    def parse_match_odds(self, event_id):
        url = self.odds_url.format(event_id=event_id)
        return self.get(url).json()

    def parse_player_stat(self, ids):
        event_id, player_id = ids
        url = self.player_statistics_rul.format(event_id=event_id, player_id=player_id)
        return self.get(url).json()

if __name__ == '__main__':
    import json
    s = SofaRequests()
    cc = s.parse_lineups_event_visual(7828239)
    print(json.dumps(cc))