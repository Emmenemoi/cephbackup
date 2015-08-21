#!/usr/local/bin/python

class CephError(Exception):
    def __init__(self, pool, msg):
        self.pool = pool
        self.msg = msg
    def __str__(self):
        if self.pool == None:
            return 'Ceph error on Null pool : '+repr(self.msg)
        else:
            return 'Ceph error on pool '+self.pool.name+' : '+repr(self.msg)
