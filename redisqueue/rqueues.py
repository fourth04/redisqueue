from . import picklecompat

class project(object):

    def __init__(self, name):
        self.name = name

class Base(object):
    """Per-project base queue class"""

    def __init__(self, server, key, serializer=None):
        """Initialize per-project redis queue.

        Parameters
        ----------
        server : StrictRedis
            Redis client instance.
        key: str
            Redis key where to put and get messages.
        serializer : object
            Serializer object with ``loads`` and ``dumps`` methods.

        """
        if serializer is None:

            #  如果没有设置序列化工具的话，默认是pickle，当然我们也可以使用json
            # Backward compatibility.
            # TODO: deprecate pickle.
            serializer = picklecompat
        if not hasattr(serializer, 'loads'):
            raise TypeError("serializer does not implement 'loads' function: %r"
                            % serializer)
        if not hasattr(serializer, 'dumps'):
            raise TypeError("serializer '%s' does not implement 'dumps' function: %r"
                            % serializer)

        self.server = server
        self.key = key
        self.serializer = serializer

    def _serialize(self, obj):
        """Serialize a obj object"""
        return self.serializer.dumps(obj)

    def _unserialize(self, serialized_obj):
        """Unserialize an obj previously serialized"""
        return self.serializer.loads(serialized_obj)

    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, obj):
        """Push a obj"""
        raise NotImplementedError

    def pop(self, timeout=0):
        """Pop a obj"""
        raise NotImplementedError

    def clear(self):
        """Clear queue/stack"""
        self.server.delete(self.key)


class FifoQueue(Base):
    """Per-project FIFO queue
    入队列的时候使用lpush，从左边开始入
    弹出队列的时候使用rpop，从右边开始弹
    从而实现：先入先出
    """

    def __len__(self):
        """Return the length of the queue"""
        return self.server.llen(self.key)

    def push(self, obj):
        """Push a obj"""
        self.server.lpush(self.key, self._serialize(obj))

    def pop(self, timeout=0):
        """Pop a obj"""
        if timeout > 0:
            #  如果设置了超时时间，就是用brpop，b应该代表block，表示阻塞到timeout时间结束
            data = self.server.brpop(self.key, timeout)
            if isinstance(data, tuple):
                data = data[1]
        else:
            data = self.server.rpop(self.key)
        if data:
            return self._unserialize(data)


class PriorityQueue(Base):
    """Per-project priority queue abstraction using redis' sorted set"""

    def __len__(self):
        """Return the length of the queue"""
        return self.server.zcard(self.key)

    def push(self, obj):
        """Push a obj"""
        data = self._serialize(obj)
        score = -obj.priority
        # We don't use zadd method as the order of arguments change depending on
        # whether the class is Redis or StrictRedis, and the option of using
        # kwargs only accepts strings, not bytes.
        self.server.execute_command('ZADD', self.key, score, data)

    def pop(self, timeout=0):
        """
        Pop a obj
        timeout not support in this queue class
        """
        # use atomic range/remove using multi/exec
        pipe = self.server.pipeline()
        pipe.multi()
        pipe.zrange(self.key, 0, 0).zremrangebyrank(self.key, 0, 0)
        results, count = pipe.execute()
        if results:
            return self._unserialize(results[0])


class LifoQueue(Base):
    """Per-project LIFO queue.
    入队列的时候使用lpush，从左边开始入
    弹出队列的时候使用lpop，从左边开始弹
    从而实现：后入先出
    """

    def __len__(self):
        """Return the length of the stack"""
        return self.server.llen(self.key)

    def push(self, obj):
        """Push a obj"""
        self.server.lpush(self.key, self._serialize(obj))

    def pop(self, timeout=0):
        """Pop a obj"""
        if timeout > 0:
            #  如果设置了超时时间，就是用brpop，b应该代表block，表示阻塞到timeout时间结束
            data = self.server.blpop(self.key, timeout)
            if isinstance(data, tuple):
                data = data[1]
        else:
            data = self.server.lpop(self.key)

        if data:
            return self._unserialize(data)


# TODO: Deprecate the use of these names.
FifoRedisQueue = FifoQueue
LifoRedisQueue = LifoQueue
PriorityRedisQueue = PriorityQueue
