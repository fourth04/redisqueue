import logging
import time

from .utils import obj_fingerprint
from .connection import get_redis_from_settings


logger = logging.getLogger(__name__)


# TODO: Rename class to RedisDupeFilter.
class RFPDupeFilter(object):
    """Redis-based obj duplicates filter.

    This class can also be used with default Scrapy's scheduler.

    """

    logger = logger

    def __init__(self, server, key, debug=False):
        """Initialize the duplicates filter.

        Parameters
        ----------
        server : redis.StrictRedis
            The redis server instance.
        key : str
            Redis key Where to store fingerprints.
        debug : bool, optional
            Whether to log filtered objs.

        """
        self.server = server
        self.key = key
        self.debug = debug
        self.logdupes = True

    @classmethod
    def from_settings(cls, settings):
        """Returns an instance from given settings.

        This uses by default the key ``dupefilter:<timestamp>``. When using the
        ``scrapy_redis.scheduler.Scheduler`` class, this method is not used as
        it needs to pass the spider name in the key.

        Parameters
        ----------
        settings : scrapy.settings.Settings

        Returns
        -------
        RFPDupeFilter
            A RFPDupeFilter instance.


        """
        server = get_redis_from_settings(settings)
        # XXX: This creates one-time key. needed to support to use this
        # class as standalone dupefilter with scrapy's default scheduler
        # if scrapy passes spider on open() method this wouldn't be needed
        # TODO: Use SCRAPY_JOB env as default and fallback to timestamp.
        key = settings.get('DUPEFILTER_KEY', 'dupefilter:%(timestamp)s' % {'timestamp': int(time.time())})
        debug = settings.get('DUPEFILTER_DEBUG', False)
        return cls(server, key=key, debug=debug)

    def obj_seen(self, obj):
        """Returns True if obj was already seen.

        Parameters
        ----------
        obj : scrapy.http.obj

        Returns
        -------
        bool

        """
        fp = self.obj_fingerprint(obj)
        # This returns the number of values added, zero if already exists.
        added = self.server.sadd(self.key, fp)
        return added == 0

    def obj_fingerprint(self, obj):
        """Returns a fingerprint for a given obj.

        Parameters
        ----------
        obj : scrapy.http.obj

        Returns
        -------
        str

        """
        return obj_fingerprint(obj)

    def __len__(self):
        """Return the length of the queue"""
        return self.server.scard(self.key)

    def close(self, reason=''):
        """Delete data on close. Called by Scrapy's scheduler.

        Parameters
        ----------
        reason : str, optional

        """
        self.clear()

    def clear(self):
        """Clears fingerprints data."""
        self.server.delete(self.key)

    def log(self, obj):
        """Logs given obj.

        Parameters
        ----------
        obj : scrapy.http.obj
        spider : scrapy.spiders.Spider

        """
        if self.debug:
            msg = "Filtered duplicate obj: %(obj)s"
            self.logger.info(msg, {'obj': obj})
        elif self.logdupes:
            msg = ("Filtered duplicate obj %(obj)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.info(msg, {'obj': obj})
            self.logdupes = False
