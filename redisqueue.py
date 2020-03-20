class RedisQueue(object):
    """ A queue class
    """
    
    def __init__(self, connection, name='queue'):
        self.redis = connection
        self.name = name
    
    def put(self, item, infirst=False):
        """ Put item into the queue.
        """             
        if infirst:
            return self.redis.lpush(self.name, item)
        else:
            return self.redis.rpush(self.name, item)
           
    def get(self, infirst=True, timeout=0):
        """ Remove and return an item from the queue or block.
        """
        try:
            if infirst:
                return self.redis.blpop(self.name, timeout)[1]
            else:
                return self.redis.brpop(self.name, timeout)[1]
        except TypeError:
            return None
        
    def get_now(self, infirst=True):
        """ Remove and return an item from the queue.
        """
        if infirst:
            return self.redis.lpop(self.name)
        else:
            return self.redis.rpop(self.name)
    
    def clear(self):
        """ Clear all items from queue.
        """
        pass
    
    def size(self):
        """ Return size of the queue.
        """
        return self.redis.llen(self.name)