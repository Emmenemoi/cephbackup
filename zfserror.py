#!/usr/local/bin/python

class ZFSError(Exception):
    def __init__(self, zpool, msg):
        self.zpool = zpool
        self.msg = msg
    def __str__(self):
        if self.zpool == None:
            return 'ZFS error on Null zpool : '+repr(self.msg)
        else:
            return 'ZFS error on zpool '+self.zpool.name+' : '+repr(self.msg)
