import importlib
import six
import time

from .utils import load_object
from . import connection
import logging

class Scheduler(object):
    """Redis-based scheduler
    连接一个redis-queue的Scheduler
    """

    def __init__(self, server,
                 persist=True,
                 flush_on_start=False,
                 queue_key='queue:%(timestamp)s' % {'timestamp': int(time.time())},
                 queue_cls='redisqueue.rqueues.FifoQueue',
                 idle_before_close=0,
                 serializer=None):
        """Initialize scheduler.

        Parameters
        ----------
        server : Redis
            The redis server instance.
        persist : bool
            Whether to flush requests when closing. Default is False.
        flush_on_start : bool
            Whether to flush requests on start. Default is False.
        queue_key : str
            Requests queue key.
        queue_cls : str
            Importable path to the queue class.
        idle_before_close : int
            Timeout before giving up.

        注意：server是redis server instance
        """
        if idle_before_close < 0:
            raise TypeError("idle_before_close cannot be negative")

        self.server = server
        self.persist = persist
        self.flush_on_start = flush_on_start
        self.queue_key = queue_key
        self.queue_cls = queue_cls
        self.idle_before_close = idle_before_close
        self.serializer = serializer
        self.logger = logging.getLogger(self.__class__.__name__)

    def __len__(self):
        return len(self.queue)

    @classmethod
    def from_settings(cls, settings):
        """
        settings
        --------
        SCHEDULER_PERSIST : bool (default: False)
            Whether to persist or clear redis queue.
        SCHEDULER_FLUSH_ON_START : bool (default: False)
            Whether to flush redis queue on start.
        SCHEDULER_QUEUE_KEY : str
            Scheduler redis key.
        SCHEDULER_QUEUE_CLASS : str
            Scheduler queue class.
        SCHEDULER_IDLE_BEFORE_CLOSE : int (default: 0)
            How many seconds to wait before closing if no message is received.
        SCHEDULER_SERIALIZER : str
            Scheduler serializer.

        注意：这里不传入server，server由程序根据settings自动生成，所以settings里还需要redis的相关连接信息，具体的请查看connection.py模块的get_redis_from_settings方法文档
        """
        kwargs = {
            'persist': settings.get('SCHEDULER_PERSIST', True),
            'flush_on_start': settings.get('SCHEDULER_FLUSH_ON_START', False),
            'queue_key': settings.get('SCHEDULER_QUEUE_KEY', 'queue:%(timestamp)s' % {'timestamp': int(time.time())}),
            'queue_cls': settings.get('SCHEDULER_QUEUE_CLASS', 'redisqueue.rqueues.FifoQueue'),
            'idle_before_close': settings.get('SCHEDULER_IDLE_BEFORE_CLOSE', 0),
            'serializer': settings.get('SCHEDULER_SERIALIZER', None)
        }

        # Support serializer as a path to a module.
        if isinstance(kwargs.get('serializer'), six.string_types):
            kwargs['serializer'] = importlib.import_module(kwargs['serializer'])

        server = connection.from_settings(settings)
        # Ensure the connection is working.
        server.ping()

        return cls(server=server, **kwargs)

    def open(self):
        try:
            self.queue = load_object(self.queue_cls)(
                server=self.server,
                key=self.queue_key,
                serializer=self.serializer,
            )
        except TypeError as e:
            raise ValueError("Failed to instantiate queue class '%s': %s", self.queue_cls, e)

        if self.flush_on_start:
            self.flush()
        # notice if there are objs already in the queue to resume the project
        if len(self.queue):
            self.logger.info("Resuming project (%d objs scheduled in %s)" % (len(self.queue), self.queue_key))

    def close(self):
        if not self.persist:
            self.flush()

    def flush(self):
        self.queue.clear()

    def enqueue(self, obj):
        self.queue.push(obj)
        return True

    def dequeue(self):
        block_pop_timeout = self.idle_before_close
        obj = self.queue.pop(block_pop_timeout)
        if obj:
            return obj

class PipeScheduler(object):

    """
    Redis-based scheduler
    连接两个redis-queue的scheduler，本调度器主要起一个管道的作用，中间会定义一些逻辑来过滤、转换等
    """

    def __init__(self, server,
                 persist=False,
                 flush_on_start=False,
                 queue_in_key='queue_in:%(timestamp)s' % {'timestamp': int(time.time())},
                 queue_in_cls='redisqueue.rqueues.FifoQueue',
                 queue_out_key='queue_out:%(timestamp)s' % {'timestamp': int(time.time())},
                 queue_out_cls='redisqueue.rqueues.FifoQueue',
                 idle_before_close=0,
                 serializer=None):
        """Initialize scheduler.

        Parameters
        ----------
        server : Redis
            The redis server instance.
        persist : bool
            Whether to flush requests when closing. Default is False.
        flush_on_start : bool
            Whether to flush requests on start. Default is False.
        queue_in_key : str
            Requests queue key.
        queue_in_cls : str
            Importable path to the queue class.
        queue_out_key : str
            Requests queue key.
        queue_out_cls : str
            Importable path to the queue class.
        idle_before_close : int
            Timeout before giving up.

        注意：server是redis server instance
        """
        if idle_before_close < 0:
            raise TypeError("idle_before_close cannot be negative")

        self.server = server
        self.persist = persist
        self.flush_on_start = flush_on_start
        self.queue_in_key = queue_in_key
        self.queue_in_cls = queue_in_cls
        self.queue_out_key = queue_out_key
        self.queue_out_cls = queue_out_cls
        self.idle_before_close = idle_before_close
        self.serializer = serializer
        self.logger = logging.getLogger(self.__class__.__name__)

    def __len__(self):
        return len(self.queue_in) + len(self.queue_out)

    @classmethod
    def from_settings(cls, settings):
        """
        settings
        --------
        SCHEDULER_PERSIST : bool (default: False)
            Whether to persist or clear redis queue.
        SCHEDULER_FLUSH_ON_START : bool (default: False)
            Whether to flush redis queue on start.
        SCHEDULER_QUEUE_IN_KEY : str
            Scheduler redis key.
        SCHEDULER_QUEUE_IN_CLASS : str
            Scheduler queue class.
        SCHEDULER_QUEUE_OUT_KEY : str
            Scheduler redis key.
        SCHEDULER_QUEUE_OUT_CLASS : str
            Scheduler queue class.
        SCHEDULER_IDLE_BEFORE_CLOSE : int (default: 0)
            How many seconds to wait before closing if no message is received.
        SCHEDULER_SERIALIZER : str
            Scheduler serializer.

        注意：这里不传入server，server由程序根据settings自动生成，所以settings里还需要redis的相关连接信息，具体的请查看connection.py模块的get_redis_from_settings方法文档
        """
        kwargs = {
            'persist': settings.get('SCHEDULER_PERSIST', True),
            'flush_on_start': settings.get('SCHEDULER_FLUSH_ON_START', False),
            'queue_in_key': settings.get('SCHEDULER_QUEUE_IN_KEY', 'queue_in:%(timestamp)s' % {'timestamp': int(time.time())}),
            'queue_in_cls': settings.get('SCHEDULER_QUEUE_IN_CLASS', 'redisqueue.rqueues.FifoQueue'),
            'queue_out_key': settings.get('SCHEDULER_QUEUE_OUT_KEY', 'queue_out:%(timestamp)s' % {'timestamp': int(time.time())}),
            'queue_out_cls': settings.get('SCHEDULER_QUEUE_OUT_CLASS', 'redisqueue.rqueues.FifoQueue'),
            'idle_before_close': settings.get('SCHEDULER_IDLE_BEFORE_CLOSE', 0),
            'serializer': settings.get('SCHEDULER_SERIALIZER', None)
        }

        # Support serializer as a path to a module.
        if isinstance(kwargs.get('serializer'), six.string_types):
            kwargs['serializer'] = importlib.import_module(kwargs['serializer'])

        server = connection.from_settings(settings)
        # Ensure the connection is working.
        server.ping()

        return cls(server=server, **kwargs)

    def open(self):
        try:
            self.queue_in = load_object(self.queue_in_cls)(
                server=self.server,
                key=self.queue_in_key,
                serializer=self.serializer,
            )
        except TypeError as e:
            raise ValueError("Failed to instantiate queue_in class '%s': %s", self.queue_in_cls, e)

        try:
            self.queue_out = load_object(self.queue_out_cls)(
                server=self.server,
                key=self.queue_out_key,
                serializer=self.serializer,
            )
        except TypeError as e:
            raise ValueError("Failed to instantiate queue_out class '%s': %s", self.queue_out_cls, e)

        if self.flush_on_start:
            self.flush()
        # notice if there are objs already in the queue to resume the project
        if len(self.queue_in) or len(self.queue_out):
            self.logger.info("Resuming project (%d objs scheduled in %s)" % (len(self.queue_in), self.queue_in_key))
            self.logger.info("Resuming project (%d objs scheduled in %s)" % (len(self.queue_out), self.queue_out_key))

    def close(self):
        if not self.persist:
            self.flush()

    def flush(self):
        self.queue_in.clear()
        self.queue_out.clear()

    def enqueue(self, obj, queue_name='out'):
        if queue_name == 'in':
            self.queue_in.push(obj)
        elif queue_name == 'out':
            self.queue_out.push(obj)
        return True

    def dequeue(self, queue_name='in'):
        block_pop_timeout = self.idle_before_close
        if queue_name == 'out':
            obj = self.queue_out.pop(block_pop_timeout)
        elif queue_name == 'in':
            obj = self.queue_in.pop(block_pop_timeout)
        else:
            obj = None
        if obj:
            return obj

    def pipe(self):
        obj = self.dequeue('in')
        if obj:
            return self.enqueue(obj, 'out')
        else:
            return False


class DupeFilterScheduler(Scheduler):

    """
    Redis-based scheduler
    连接两个redis-queue的scheduler，其中一个用于去重，另一个用于保存
    """

    def __init__(self, server,
                 persist=False,
                 flush_on_start=False,
                 queue_key='queue:%(timestamp)s' % {'timestamp': int(time.time())},
                 queue_cls='redisqueue.rqueues.FifoQueue',
                 dupefilter_key='dupefilter:%(timestamp)s' % {'timestamp': int(time.time())},
                 dupefilter_cls='redisqueue.dupefilter.RFPDupeFilter',
                 dupefilter_debug=False,
                 idle_before_close=0,
                 serializer=None):
        """Initialize scheduler.

        Parameters
        ----------
        server : Redis
            The redis server instance.
        persist : bool
            Whether to flush requests when closing. Default is False.
        flush_on_start : bool
            Whether to flush requests on start. Default is False.
        queue_key : str
            Requests queue key.
        queue_cls : str
            Importable path to the queue class.
        dupefilter_key : str
            Duplicates filter key.
        dupefilter_cls : str
            Importable path to the dupefilter class.
        dupefilter_debug : bool
            Do you need to show the debug information
        idle_before_close : int
            Timeout before giving up.

        注意：server是redis server instance
        """
        Scheduler.__init__(self, server=server, persist=persist, flush_on_start=flush_on_start, queue_key=queue_key, queue_cls=queue_cls, idle_before_close=idle_before_close, serializer=serializer)

        self.dupefilter_key = dupefilter_key
        self.dupefilter_cls = dupefilter_cls
        self.dupefilter_debug = dupefilter_debug
        self.logger = logging.getLogger(self.__class__.__name__)

    @classmethod
    def from_settings(cls, settings):
        """
        settings
        --------
        SCHEDULER_PERSIST : bool (default: False)
            Whether to persist or clear redis queue.
        SCHEDULER_FLUSH_ON_START : bool (default: False)
            Whether to flush redis queue on start.
        SCHEDULER_QUEUE_KEY : str
            Scheduler redis key.
        SCHEDULER_QUEUE_CLASS : str
            Scheduler queue class.
        SCHEDULER_DUPEFILTER_KEY : str
            Scheduler dupefilter redis key.
        SCHEDULER_DUPEFILTER_CLASS : str
            Scheduler dupefilter class.
        SCHEDULER_DUPEFILTER_DEBUG : str
            Scheduler dupefilter debug flag.
        SCHEDULER_IDLE_BEFORE_CLOSE : int (default: 0)
            How many seconds to wait before closing if no message is received.
        SCHEDULER_SERIALIZER : str
            Scheduler serializer.

        注意：这里不传入server，server由程序根据settings自动生成，所以settings里还需要redis的相关连接信息，具体的请查看connection.py模块的get_redis_from_settings方法文档
        """
        kwargs = {
            'persist': settings.get('SCHEDULER_PERSIST', True),
            'flush_on_start': settings.get('SCHEDULER_FLUSH_ON_START', False),
            'queue_key': settings.get('SCHEDULER_QUEUE_KEY', 'queue:%(timestamp)s' % {'timestamp': int(time.time())}),
            'queue_cls': settings.get('SCHEDULER_QUEUE_CLASS', 'redisqueue.rqueues.FifoQueue'),
            'dupefilter_key': settings.get('SCHEDULER_DUPEFILTER_KEY', 'dupefilter:%(timestamp)s' % {'timestamp': int(time.time())}),
            'dupefilter_cls': settings.get('SCHEDULER_DUPEFILTER_CLASS', 'redisqueue.dupefilter.RFPDupeFilter'),
            'dupefilter_debug': settings.get('SCHEDULER_DUPEFILTER_DEBUG', False),
            'idle_before_close': settings.get('SCHEDULER_IDLE_BEFORE_CLOSE', 0),
            'serializer': settings.get('SCHEDULER_SERIALIZER', None)
        }

        # Support serializer as a path to a module.
        if isinstance(kwargs.get('serializer'), six.string_types):
            kwargs['serializer'] = importlib.import_module(kwargs['serializer'])

        server = connection.from_settings(settings)
        # Ensure the connection is working.
        server.ping()

        return cls(server=server, **kwargs)

    def open(self):
        try:
            self.queue = load_object(self.queue_cls)(
                server=self.server,
                key=self.queue_key,
                serializer=self.serializer,
            )
        except TypeError as e:
            raise ValueError("Failed to instantiate queue class '%s': %s", self.queue_cls, e)

        try:
            self.df = load_object(self.dupefilter_cls)(
                server=self.server,
                key=self.dupefilter_key,
                debug=self.dupefilter_debug
            )
        except TypeError as e:
            raise ValueError("Failed to instantiate dupefilter class '%s': %s", self.dupefilter_cls, e)

        if self.flush_on_start:
            self.flush()
        # notice if there are objs already in the queue to resume the project
        if len(self.df) or len(self.queue):
            self.logger.info("Resuming project (%d objs scheduled in %s)" % (len(self.df), self.dupefilter_key))
            self.logger.info("Resuming project (%d objs scheduled in %s)" % (len(self.queue), self.queue_key))

    def flush(self):
        self.df.clear()
        self.queue.clear()

    def enqueue(self, obj):
        if self.df.obj_seen(obj):
            self.df.log(obj)
            return False
        else:
            self.queue.push(obj)
            return True

    def dequeue(self):
        block_pop_timeout = self.idle_before_close
        obj = self.queue.pop(block_pop_timeout)
        if obj:
            return obj

