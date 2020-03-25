# Common Python library imports
from datetime import date, timedelta
import multiprocessing as mp
from contextlib import contextmanager
import multiprocessing.queues as mpq
import time

# Pip package imports
from selenium import webdriver
from loguru import logger

# Internal package imports
from miner.utils import convert_datetime, date_interval, get_nested, Singleton, ObjectMaker

DEFAULT_MODEL_NAME = "undefined"
DEFAULT_MODEL_VERSION = "v0_1"

class IHandler(object):

    config = {
        'logging': True,
        'num_of_threads' : 8
    }

    def __init__(self, name=DEFAULT_MODEL_NAME, slug=DEFAULT_MODEL_NAME, version=DEFAULT_MODEL_VERSION, *args,
                 **kwargs):
        self._name = name
        self._slug = slug
        self._version = version

        self._converter = kwargs.get('converter', Converter)

        self._config = { **IHandler.config , **kwargs.get('config', {})}

    def fetch_dates(self, *args, **kwargs):
        # Get the input parameters
        start = convert_datetime(kwargs.get('start', date.today()))
        end = convert_datetime(kwargs.get('end', start + timedelta(days=0)))

        start_time = time.time()
        result = self._do_fetch(start, end, **kwargs)
        time_took = (time.time() - start_time)
        if self._get_config('logging'):
            logger.info("[%s] fetching data from %s to %s took %0.2f sec." % (self._name, start, end, time_took))
        return result

    def _do_fetch(self, start_date, end_date, *args, **kwargs):
        for curr_date in date_interval(start_date, end_date):
            yield self._fetch_date(curr_date, **kwargs)

    def _fetch_date(self, curr_date, *args, **kwargs):
        pass

    def _get_config(self, *args):
        return get_nested(self._config, *args)

    def info(self, msg):
        name = "[%s] " % self._name
        logger.info(name + msg)

    def warn(self, msg):
        name = "[%s] " % self._name
        logger.warn(name + msg)

    def error(self, msg):
        name = "[%s] " % self._name
        logger.error(name + msg)




class Converter(object):

    def __init__(self, *args, **kwargs):
        pass

    def __del__(self):
        try:
            self.get()
        except Exception:
            pass

    def put(self, object):
        pass

    def get(self):
        return None


class DriverPool(mpq.Queue, metaclass=Singleton):

    def __init__(self, maker=None, *args, **kwargs):
        size = kwargs.get('size', 10)
        self.name = kwargs.get('name', "Unknown")
        ctx = mp.get_context()
        super(DriverPool, self).__init__(size, ctx=ctx)
        if maker is not None:
            self.setup(maker)

    def __del__(self):
        self.destroy()

    @contextmanager
    def get_context(self, block=True, timeout=None):
        element = self.get(block, timeout)
        try:
            yield element
        except Exception as err:
            logger.error(err)
        finally:
            self.put(element, block, timeout)

    def setup(self, maker):
        assert isinstance(maker, ObjectMaker), "input argument \'maker\' must be an ObjectMaker object."
        logger.debug("Filling [%s] pool with [%s]" % (self.name, maker.class_))
        while not self.full():
            try:
                self.put(maker(), timeout=1)
            except Exception as err:
                logger.error(err)

    def destroy(self):
        logger.debug("Emptying [%s] pool" % (self.name))
        while not self.empty():
            try:
                obj = self.get(timeout=1)
                del obj
            except Exception as err:
                logger.error(err)


def get_firefox_driver(**kwargs):
    from selenium.webdriver.firefox.options import Options

    driver_path = kwargs.get('driver_path', "")
    log_path = kwargs.get('log_path', "./webdriver.log")
    options = kwargs.get('options', Options())
    profile = kwargs.get('profile', webdriver.FirefoxProfile())
    options.headless = kwargs.get('headless', False)
    profile.set_preference('intl.accept_languages', 'en')

    args = {
        'service_log_path': log_path,
        'options': options,
        'firefox_profile': profile,
        'class_' : webdriver.Firefox,
    }
    if len(driver_path) > 0:
        args['executable_path'] = driver_path

    return args

def get_chrome_driver(**kwargs):
    from selenium.webdriver.chrome.options import Options

    driver_path = kwargs.get('driver_path', "")
    log_path = kwargs.get('log_path', "./log/webdriver.log")
    options = kwargs.get('options', Options())
    options.headless = kwargs.get('headless', False)

    args = {
        'service_log_path': log_path,
        'options': options,
        'class_': webdriver.Chrome,
    }
    if len(driver_path) > 0:
        args['executable_path'] = driver_path

    return args



