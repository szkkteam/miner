# Common Python library imports
import traceback
import dateutil.parser
import re
import copy

# Pip package imports
import requests
from requests.exceptions import Timeout, HTTPError
from lxml import html
from loguru import logger

# Internal package imports
from miner.utils import retry, convert_datetime, Singleton, get_nested

__all__ = ["SofaScoreScrapper", "FifaScrapper"]

default_stats = { 'Ball Control': "", 'Dribbling': "" , # Ball Skills
                  'Marking': "", 'Slide Tackle': "", 'Stand Tackle': "" , # Defence
                  'Aggression': "", 'Reactions': "", 'Att. Position': "", 'Interceptions': "", 'Vision': "", 'Composure': "", # Mental
                  'Crossing': "", 'Short Pass': "", 'Long Pass': "" , # Passing
                  'Acceleration': "", 'Stamina': "", 'Strength': "", 'Balance': "", 'Sprint Speed': "", 'Agility': "", 'Jumping': "", # Physical
                  'Heading': "", 'Shot Power': "", 'Finishing': "", 'Long Shots': "", 'Curve': "", 'FK Acc.': "", 'Penalties': "", 'Volleys': "", # Shooting
                  'GK Positioning': "", 'GK Diving': "", 'GK Handling': "", 'GK Kicking': "", 'GK Reflexes': "" , # Goalkeeper
                  'Overall': "", 'Potential': "", # Rating
                  'Height': "", 'Weight': "", 'Preferred Foot': "", 'Preferred Positions': "" }


@retry(Timeout, tries=4, delay=2)
def open_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
    except HTTPError as err:
        print(err)
        return None
    return response.content

class SofaScoreScrapper(metaclass=Singleton):

    default_config = {

    }

    player_url = "https://www.sofascore.com/player/filipe-luis/{player_id}"

    def __init__(self, *args, **kwargs):
        self._config = { **SofaScoreScrapper.default_config , **kwargs.get('config', {})}

    def parse_player_birtdate(self, player_id):
        url = SofaScoreScrapper.player_url.format(player_id=player_id)
        content = open_url(url)

        sc_player_birth_date = None
        try:
            tree = html.fromstring(content)
            select_1 = tree.xpath('//div[contains(@class, "cell u-tC u-flex-wrap u-mT32")]')[0]
            try:
                select_2 = select_1.xpath(".//*[contains(text(), 'Age')]")[0]
            except Exception:
                try:
                    select_2 = select_1.xpath(".//*[contains(text(), 'Alter')]")[0]
                except Exception:
                    select_2 = select_1.xpath(".//*[contains(text(), 'birth')]")[0]
            select_3 = select_2.xpath('..')[0]
            try:
                x = select_3.xpath('.//div[contains(@class, "cell__content ff-medium u-fs15")]')[0].text
                try:
                    x = re.search(r"\((.+)\)", x).group(0)
                    x = x[x.find("(") + 1:x.find(")")]
                except Exception:
                    pass
                finally:
                    sc_player_birth_date = dateutil.parser.parse(x)
                    sc_player_birth_date = sc_player_birth_date.date()
            except ValueError as err:
                # This type of error is not interesting, because some players dosen't have date information
                pass
            except Exception as err:
                tb = traceback.format_exc()
                logger.error(tb)
                raise
        except Exception as err:
            logger.error("Failed with player id: %s Error: %s" % (player_id, err))

        return (player_id, sc_player_birth_date )


class FifaScrapper(object):

    default_config = {
        'id_list': {'Samuel Radlinger-Sahin': '193272',
                    'Tomáš Vaclík': '204120',
                        'Bryan Salvatierra': '246785',
                    'Clinton N\'Jie': '212273',
                    'Pierre Lees Melou': '230020',
                    'Nicolas N\'Koulou':  '188829',
                    'Jóhann Guðmundsson': '191076',
                    'Igniatius Ganago': '241130',
                    'Heung-Min Son': '200104',
                    'Rúnar Alex': '222562' },
        'exclude_player_stats' : ['Traits', 'Specialities'],
        'full_search': True

    }

    top_url = "https://www.fifaindex.com/players/top/"
    player_url = "https://www.fifaindex.com/player/{id}"
    search_url = "https://www.fifaindex.com/players/?name={name}&order=desc"
    change_log = "https://www.fifaindex.com/player/{id}/{name}/changelog/"


    def __init__(self, *args, **kwargs):
        self._config = { **FifaScrapper.default_config , **kwargs.get('config', {})}


    def _get_player_stat(self, fifa_id):
        content = open_url(FifaScrapper.player_url.format(id=fifa_id))
        tree = html.fromstring(content)

        tempdict = copy.deepcopy(default_stats)
        try:
            select_1 = tree.xpath('//div[contains(@class, "row pt-3")]')[0]
            select_2 = select_1.xpath('.//div[contains(@class, "col-sm-6")]')
            changelog = select_1[1].xpath('.//a[@class="btn btn-block btn-sm btn-primary mt-3"]')[0]
            changelog_url = "https://www.fifaindex.com" + changelog.get('href')
        except Exception as err:
            logger.error(err)
        else:
            if len(select_2) >= 1:
                try:
                    rating = select_2[1].xpath('.//span[contains(@class, "rating")]')
                    # Get ratings:
                    tempdict['Overall'] = rating[0].text
                    tempdict['Potential'] = rating[1].text
                except Exception as err:
                    logger.error(err)
                try:
                    select_Weight = select_2[1].xpath(".//*[contains(text(), 'Height')]")[0]
                    Height = select_Weight.xpath('.//span[contains(@class, "data-units data-units-metric")]')[0]
                    # Get height
                    tempdict["Height"] = re.findall(r"(\d+)", Height.text)[0]
                except Exception as err:
                    logger.error(err)

                select_Weight = select_2[1].xpath(".//*[contains(text(), 'Weight')]")[0]
                Weight = select_Weight.xpath('.//span[contains(@class, "data-units data-units-metric")]')[0]
                # Get weight
                tempdict["Weight"] = re.findall(r"(\d+)", Weight.text)[0]

                select_preferredfoot = select_2[1].xpath(".//*[contains(text(), 'Preferred Foot')]")[0]
                preferred_foot = select_preferredfoot.xpath('.//span[contains(@class, "float-right")]')[0]
                # Get preferred foot
                tempdict["Preferred Foot"] = preferred_foot.text

                select_preferredpositions = select_2[1].xpath(
                    ".//*[contains(text(), 'Preferred Positions')]")[0]
                preferred_positions = select_preferredpositions.xpath(
                    './/a[contains(@class, "link-position")]')

                # Get preferred positions
                positions = []
                for pos in preferred_positions:
                    pos_name = pos.get("title")
                    if pos_name is None:
                        pos_name = ""
                    positions.append(pos_name)
                tempdict['Preferred Positions'] = positions
            else:
                logger.warn("select_2 dosen't have enough element. Length: %s" % len(select_2))

            try:
                select_stats_1 = tree.xpath('//div[contains(@class, "row grid")]/div')
                #select_stats_2 = select_stats_1.xpath('.//div')
                #select_stats_2 = select_stats_1.xpath('.//h5[contains(@class, "card-header")]')
            except Exception as err:
                logger.error(err)
            else:
                for stat in select_stats_1:
                    try:
                        select_elements_1 = stat.xpath('.//div[@class="card-body"]/p')
                        #select_elements_2 = select_elements_1.xpath('p')
                        for element in select_elements_1:
                            try:
                                values = element.xpath('.//text()')
                                if len(values) <= 1:
                                    continue
                                stat_name = values[0].strip()
                                stat_value = int(values[-1].strip())
                                tempdict[stat_name] = stat_value
                            except Exception as err:
                                logger.error(err)
                    except Exception as err:
                        logger.error(err)
                        continue
            return tempdict, changelog_url

    def _get_old_player_stat(self, row, previous_stat):
        log_date = row.xpath('.//h5[@class="card-header"]//a[contains(@href, "player")]')[0]
        log_date = log_date.text.split("@ ")
        log_date = dateutil.parser.parse(log_date[1]).date()

        tempdict = copy.deepcopy(previous_stat)
        try:
            changes = row.xpath('.//div[@class="mb-2 col-6"]')
            for change in changes:
                #print("--------------- New Element -----------")
                #print("\tRaw: \'%s\'" % change.text)
                try:
                    try:
                        stat_name, stat_value = change.text.split(":")
                        stat_name = stat_name.strip()
                        stat_value = stat_value.split()[0].strip()
                    except Exception as err:
                        continue
                    if stat_value == "N/A":
                        stat_value = ""

                    if "Preferred Position" in stat_name:
                        pos = int(re.findall(r"(\d+)", stat_name)[0])
                        while ((len( tempdict['Preferred Positions'])) < (pos) ):
                            tempdict['Preferred Positions'].append("")

                        tempdict['Preferred Positions'][pos - 1] = stat_value

                    if stat_name in tempdict:
                        tempdict[stat_name] = stat_value
                except Exception as err:
                    logger.error(err)
                    continue

        except Exception as err:
            logger.error(err)

        return log_date, tempdict

    def _get_player_id(self, url_list, player_birth_date):

        for url, id in url_list:
            try:
                content = open_url(url)
                tree = html.fromstring(content)

                select_1 = tree.xpath('//div[contains(@class, "row pt-3")]')[0]
                select_2 = select_1.xpath(".//p[contains(text(), 'Birth Date')]")[0]
                fifa_birth_date = select_2.xpath(".//span[contains(@class, 'float-right')]")[0].text

                fifa_birth_date = dateutil.parser.parse(fifa_birth_date)
                fifa_birth_date = convert_datetime(fifa_birth_date)

                if len(url_list) > 1:
                    if fifa_birth_date == player_birth_date:
                        return id
                else:
                    return id
            except Exception as err:
                logger.error(err)
                continue
        return None

    def _find_by_name(self, player_name, player_birthdate):
        if player_name is None:
            return None
        try:
            # Make a search request to Fifa and get the response html
            fixed_player_name = player_name.replace(' ', '+')
            content = open_url(FifaScrapper.search_url.format(name=fixed_player_name))
            if content is None:
                return None
            tree = html.fromstring(content)

            table_frame = tree.xpath('//table[contains(@class, "table table-striped table-players")]')[0]
            rows = table_frame.xpath('//tr/@data-playerid')
            url_list = [ (FifaScrapper.player_url.format(id=id), id) for id in rows ]

            fifa_player_id = self._get_player_id(url_list, player_birthdate)
            return fifa_player_id
        except Exception as err:
            tb = traceback.format_exc()
            logger.error(tb)
            logger.error("Error happened: [%s] when tried to find player name: [%s] birth: [%s]." % (err, player_name, player_birthdate ))


    def _find_fifa_player(self, **kwargs):
        # Extract the possible input parameters
        full_name = kwargs.get('full_name', None)
        short_name = kwargs.get('short_name', None)
        birth_day = kwargs.get('birthday', None)

        # Lookup in the alias list
        if full_name in self._get_config('id_list'):
            return self._get_config('id_list')[full_name]

        names = []
        if full_name is not None:
            names.append(full_name)
        if short_name is not None:
            names.append(short_name)
        if full_name is not None:
            names.extend(full_name.split())

        for name in names:
            fifa_id = self._find_by_name(name, birth_day)
            if fifa_id is not None:
                break
        return fifa_id

    def parse_fifa_stats(self, **kwargs):
        player_birthdate = convert_datetime(kwargs.get('birth', None))
        player_name = kwargs.get('name', None)
        player_short_name = kwargs.get('short', None)
        fifa_id = kwargs.get('fifa_idx', None)

        if fifa_id is None:
            # Lookup for the fifa player
            fifa_id = self._find_fifa_player(full_name=player_name,
                                             short_name=player_short_name,
                                             birthday=player_birthdate)
        try:

            if fifa_id is not None:
                stats, changelog_url = self._get_player_stat(fifa_id)

                content = open_url(changelog_url)
                tree = html.fromstring(content)

                change_log_list = tree.xpath('//div[@class="col-lg-8"]//div[@class="card mb-5"]')

                stats_list = []
                for log in change_log_list:
                    stat_date, prev_stats = self._get_old_player_stat(log, stats)
                    stats_list.append( (stat_date, stats) )
                    stats = prev_stats

                return stats_list, fifa_id
            else:
                logger.warning("Could not find Sofascore player name: [%s] short: [%s] birth: [%s]" % (player_name, player_short_name, player_birthdate))
                return None, None
        except Exception as err:
            tb = traceback.format_exc()
            logger.error(tb)
            logger.error("Error happened: [%s] when tried to find player name: [%s] birth: [%s]." % (
                            err, player_name, player_birthdate))
            return None, None

    def _get_config(self, *args):
        data = get_nested(self._config, *args)
        assert data is not None, "config \'%s\' is not supported." % args
        return data


if __name__ == '__main__':
    import json
    from datetime import date

    s = FifaScrapper()

    fifa_id = 210329
    sc_id = 142037
    full_name = "Filippo Falco"
    #full_name = "Filippo"
    short = "F. Falco"
    birth = date(1992, 2, 11)

    so = SofaScoreScrapper()
    _, birth_s = so.parse_player_birtdate(sc_id)
    print(birth, birth_s)

    """
    res, _ = s.parse_fifa_stats(fifa_idx=fifa_id, birth=birth, name=full_name, short=short)
    #res, _ = s.parse_fifa_stats(birth=birth, name=full_name, short=short)
    #res, _ = s.parse_fifa_stats(fifa_idx=fifa_id, birth=birth, name=full_name, short=short)
    for date, stat in res:
        print(date)
        print(json.dumps(stat, sort_keys = True, indent = 4))
    pass
    """
