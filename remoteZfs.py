#!/usr/local/bin/python

import sys, getopt, re
import logging
from xml.dom.minidom import parse
from subprocess import Popen, PIPE, check_output, CalledProcessError, STDOUT
from datetime import datetime, timedelta, date
from zfserror import *

class RemoteSnapshot(object):

    def __init__(self, name, dataset, dryrun=True):
        self.name = name
        self.dataset = dataset
        self.dryrun = dryrun
        self.__keep = None
        self.__keepTested = False
        self.__tags = None

    def __del__(self):
        if self.dryrun:
            self.dataset.zpool.used -= self.used
            self.dataset.zpool.available += self.used

    def isLastBackup(self):
        return "@lastbackup" in self.name

    def isCurrentBackup(self):
        return "@currentbackup" in self.name

    def renameToLastBackup(self):
        lastBackup = self.dataset.getLastBackupSnapshot()
        if lastBackup != None:
            lastBackup.rename( lastBackup.name.replace('@lastbackup','@'+lastBackup.creation.strftime("%Y-%m-%d_%H.%M.%S")) )

        return self.rename( self.name.replace('@currentbackup','@lastbackup'), True )

    def rename(self, name, force=False):
        # check if exists if forced
        if force:
            existingSnapshot = self.dataset.getSnapshot(name)
            if existingSnapshot != None:
                existingSnapshot.destroy()

        cmd = self.dataset.zpool.getSshPrefix() + ' '+' '.join(["zfs", "rename", self.name, name])
        if self.dryrun:            
            result = ''
            logging.info(cmd)
        else:
            logging.debug(cmd)
            result = check_output(cmd, shell=True)
            logging.debug(result)
        if result=='':
            logging.info("Remote snapshot '%s' has been renamed to '%s'" % (self.name, name))
            self.name = name
        else:
            logging.error("Remote snapshot '%s' failed to be renamed to '%s'" % (self.name, name))     
        return result == ''

    def destroy(self):
        cmd = self.dataset.zpool.getSshPrefix() + ' '+' '.join( ["zfs", "destroy", "-d", self.name] )
        if self.dryrun:
            logging.info(cmd)
            result = ''
        else:
            logging.debug(cmd)
            result = check_output(cmd, shell=True)
            logging.debug(result)
        if result=='':
            logging.info("Remote snapshot '%s' has been destroyed" % self.name)
            self.dataset.snapshots.remove(self)
        else:
            logging.error("Remote snapshot '%s' failed to be destroyed" % self.name)
        return result==''


class RemoteZpool(object):

    def __init__(self, host, user, name, dryrun=True, ssh_key=''):
        self.host = host
        self.user = user
        self.ssh_key = ssh_key
        self.zpools = set()
        if ( self.ssh_key == '' ):
            self._sshprefix = '/usr/bin/ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -q ' + self.user + '@' + self.host
        else:
            self._sshprefix = '/usr/bin/ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -q -i '+ self.ssh_key + ' ' + self.user + '@' + self.host

        self.name = name
        self.dryrun = dryrun
        self.maxCapacity = 0.8
        self.bestEffortPolicy = "morerem"
        if self.isScrubActive() : 
            raise ZFSError(self, 'Pool is scrubbing')
        else:
            self.__used = int(check_output(self._sshprefix + ' '+' '.join(["zfs", "get", "-H", "-p", "-o", "value", "used", self.name]), shell=True)) 
            self.__available = int(check_output(self._sshprefix + ' '+' '.join( ["zfs", "get", "-H", "-p", "-o", "value", "available", self.name]), shell=True))
            self.refreshDatasets()

    def isScrubActive(self):
        result = ""
        parent = ""
        poollist = []
        for zpool in self.name.split("/"):
            poollist.append(parent + zpool)            
            parent += zpool + "/"

        # via ps aux (but scrub can continue if process killed)
        #cmd = self._sshprefix + ' '+' '.join(["ps", "-A", "|", "/bin/grep", "-v", "grep", "|", "/bin/grep", "-P", '"zpool scrub ('+"|".join(poollist)+')"', "|", "wc", "-l"])
        cmd = self._sshprefix + ' '+' '.join(["zpool", "status", "|", "/bin/grep", "-e", '"scrub in progress"', "|", "wc", "-l"]     )   
        logging.debug(cmd)
        result = int(check_output(cmd, shell=True))
        logging.debug(result)
        return result > 0

    def refreshDatasets(self):
        self.datasets = set()
        logging.info("Getting remote datasets information for zpool %s, this may take a while..." % (self.name))
        for line in check_output(self._sshprefix + ' '+' '.join(["zfs", "get", "-rHp", "type,creation,used,available,referenced,userrefs", self.name]), shell=True).split("\n"):
            if line != "":
                name = line.split()[0]
                property = line.split()[1]
                value = line.split()[2]
                if property == "type":
                    if value == "filesystem":
                        dataset = RemoteFilesystem(name, self, self.dryrun)
                        self.datasets.add(dataset)
                    elif value == "volume":
                        dataset = RemoteVolume(name, self, self.dryrun)
                        self.datasets.add(dataset)
                    elif value == "snapshot":
                        if isinstance(dataset, RemoteSnapshot):
                            parent = dataset.dataset
                        else:
                            parent = dataset
                        dataset = RemoteSnapshot(name, parent, self.dryrun)
                        parent.snapshots.append(dataset)
                elif property == "creation":
                    dataset.creation = datetime.fromtimestamp(int(value))
                elif property == "used":
                    dataset.used = int(value)
                elif property == "available":
                    try:
                        dataset.available = int(value)
                    except:
                        pass
                elif property == "referenced":
                    dataset.referenced = int(value)
                elif property == "userrefs":
                    try:
                        dataset.userrefs = int(value)
                    except:
                        pass

    def importSnapshot(self, remoteDataset, localsnapshot, incrementalSnap=None):
        if incrementalSnap != None:
            incStr = ' -i ' + incrementalSnap.name
        else:
            incStr = ''
        cmd = '/sbin/zfs send '+incStr+' '+localsnapshot.name+' | /bin/gzip -c | ' + self.getSshPrefix() + ' \"%s\"' % ('zcat | '+' '.join( ["zfs", "recv", "-F", "-d", "-u", "-v", self.name]))
        if self.dryrun:
            logging.info(cmd)
            result = 'received'
        else:
            logging.debug(cmd)
            try:
                result = check_output(cmd, stderr=STDOUT, shell=True)
                logging.debug(result)
            except CalledProcessError, e:
                result = e.output 
                logging.debug(result)
                pass
        if 'cannot receive incremental stream: most recent snapshot' in result:
            try:
                # might be conflicts in lastbackups, we remove them on both sides and restart                
                if incrementalSnap.isLastBackup() :
                    incrementalSnap.destroy()
                lastRemoteSnapshot = remoteDataset.getLastBackupSnapshot()
                if lastRemoteSnapshot != None:
                    remoteDataset.rollBackupNames()
                    #lastRemoteSnapshot.destroy()
            except: 
                pass
            lastIncrementSnapshot = localsnapshot.dataset.getMostRecentMatchingSnapshot( remoteDataset.snapshots )
            if lastIncrementSnapshot != None:
                return self.importSnapshot(remoteDataset, localsnapshot, lastIncrementSnapshot)
            else:
                return self.importSnapshot(remoteDataset, localsnapshot)
        elif 'received' in result:
            logging.info("Snapshot '%s' has been imported to %s" % (localsnapshot.name, self.name))
        else:
            logging.error("Snapshot '%s' failed to be imported to %s" % (localsnapshot.name, self.name))

        if 'received' in result:
            self.refreshDatasets()
        return 'received' in result

    def getSshPrefix(self):
        return self._sshprefix

    def getUsed(self):
        if self.dryrun:
            return self.__used
        else:
            return int(check_output(self._sshprefix + ' '+' '.join( ["zfs", "get", "-H", "-p", "-o", "value", "used", self.name]), shell=True))

    def setUsed(self, value):
        self.__used = value

    used = property(getUsed, setUsed)

    def getAvailable(self):
        if self.dryrun:
            return self.__available
        else:
            return int(check_output(self._sshprefix + ' '+' '.join( ["zfs", "get", "-H", "-p", "-o", "value", "available", self.name]), shell=True))

    def setAvailable(self, value):
        self.__available = value

    available = property(getAvailable, setAvailable)

    def getCapacity(self):
        return float(self.used) / (self.used + self.available)

    capacity = property(getCapacity)

    def getDataset(self, name):
        for dataset in self.datasets:
            if dataset.name == name:
                return dataset
        return None

    def getReferenced(self):
        referenced = 0
        for dataset in self.datasets:
            referenced += dataset.referenced
        return referenced

    referenced = property(getReferenced)

class RemoteDataset(object):

    def __init__(self, name, zpool, dryrun=True):
        self.name = name
        self.zpool = zpool
        self.dryrun = dryrun
        self.snapshots = []
        self.__maxRetention = None
        self.__retentionPolicy = None
        self.userrefs = None
        if name.count('/') > 0:
            self.parent = zpool.getDataset(name.rsplit('/', 1)[0])
        else:
            self.parent = None

    def getReferenced(self):
        if self.dryrun:
            return self.__referenced
        else:
            return int(check_output(zpool.getSshPrefix() + ' '+' '.join( ["zfs", "get", "-H", "-p", "-o", "value" , "referenced", self.name]), shell=True))

    def setReferenced(self, value):
        self.__referenced = value

    referenced = property(getReferenced, setReferenced)

    def getLastBackupSnapshot(self):
        for snapshot in self.snapshots[:]:
            if snapshot.isLastBackup():
                return snapshot
        return None

    def getCurrentBackupSnapshot(self):
        for snapshot in self.snapshots[:]:
            if snapshot.isCurrentBackup():
                return snapshot
        return None

    def getSnapshot(self, name):
        for snapshot in self.snapshots[:]:
            if snapshot.name == name:
                return snapshot
        return None

    def getMostRecentMatchingSnapshot(self, localsnapshots):
        matchingSnap = None
        for snapshot in self.snapshots[:]:
            if snapshot.isLastBackup():
                return snapshot
            elif matchingSnap == None or matchingSnap.creation < snapshot.creation:
                for localSnapshot in localsnapshots[:]:
                    if localSnapshot.creation == snapshot.creation:
                        matchingSnap = snapshot
        return matchingSnap

    def rollBackupNames(self):
        lastBackup = self.getLastBackupSnapshot()
        if lastBackup != None:
            lastBackup.rename( lastBackup.name.replace('@lastbackup','@'+lastBackup.creation.strftime("%Y-%m-%d_%H.%M.%S")) )
    
        for snapshot in self.snapshots[:]:
            if snapshot.isCurrentBackup():
                snapshot.renameToLastBackup()
            

class RemoteVolume(RemoteDataset):

    pass

class RemoteFilesystem(RemoteDataset):

    pass

