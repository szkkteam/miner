# Common Python library imports
from datetime import timedelta
import time
from functools import wraps

# Pip package imports
from loguru import logger


def listify(args):
    """Return args as a list.
    If already a list - returned as is.
    If a single instance of something that isn't a list, return it in a list.
    If "empty" (None or whatever), return a zero-length list ([]).
    """
    if args:
        if isinstance(args, list):
            return args
        return [args]
    return []


def retry(ExceptionToCheck, tries=4, delay=1, backoff=2, logger=logger):
    """Retry calling the decorated function using an exponential backoff.

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            logger.error("%s, Retrying failed.")
            return None

        return f_retry  # true decorator

    return deco_retry


def get_nested(data, *args, **kwargs):
    if args and data:
        element  = args[0]
        if element:
            try:
                value = data.get(element)
            except AttributeError as err:
                logger.error("Exeption: %s Data: %s Args: %s" % (err, data, *args))
                return kwargs.get('default', None)
            else:
                return value if len(args) == 1 else get_nested(value, *args[1:])
    return kwargs.get('default', None)

def convert_datetime(date):
    try:
        date = date.date()
    except AttributeError:
        pass
    return date

def safe_cast(value, type, default=None):
    try:
        return type(value)
    except Exception as err:
        return default

def date_interval(start, end, delta=1):
    curr = start
    while curr <= end:
        yield curr
        curr += timedelta(delta)

# Python program to illustrate the intersection
# of two lists using set() method
def intersection(lst1, lst2):
    return list(set(lst1) & set(lst2))


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        #else:
#            cls._instances[cls].__init__(*args, **kwargs)
        return cls._instances[cls]
    """
    @staticmethod
    def getInstance():
        if Singleton.__instance == None:
            Singleton()
        return Singleton.__instance
    """


class ObjectMaker(object):

    def __init__(self,
                 class_=None,
                 **kwargs):
        self._kwargs = kwargs

        self.class_ = type(class_.__name__, (class_,), {})

    def __call__(self, **local_kw):
        for k, v in self._kwargs.items():
            if k == "info" and "info" in local_kw:
                d = v.copy()
                d.update(local_kw["info"])
                local_kw["info"] = d
            else:
                local_kw.setdefault(k, v)
        return self.class_(**local_kw)

def split_into(arr, n):
    sp = len(arr) // n
    if sp == 0:
        return [[x, ] for x in arr]
    return [arr[i:i + sp] for i in range(0, len(arr), sp)]

def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))