#!/usr/local/bin/python

import sys, getopt, re
import logging
from xml.dom.minidom import parse
from subprocess import Popen, PIPE, check_output, STDOUT
from datetime import datetime, timedelta, date
from zfserror import *

class Zpool(object):

    def __init__(self, name, dryrun=True):
        self.name = name
        self.dryrun = dryrun
        self.maxCapacity = 0.8
        self.bestEffortPolicy = "morerem"
        if self.isScrubActive() : 
            raise ZFSError(self, 'Pool is scrubbing')
        else:
            self.__used = int(check_output(["/sbin/zfs", "get", "-H", "-p", "-o", "value", "used", self.name])) 
            self.__available = int(check_output(["/sbin/zfs", "get", "-H", "-p", "-o", "value", "available", self.name]))
            self.datasets = set()
            logging.info("Getting datasets information for zpool %s, this may take a while..." % (self.name))
            for line in check_output(["/sbin/zfs", "get", "-rHp", "type,creation,used,available,referenced,userrefs", self.name]).split("\n"):
                if line != "":
                    name = line.split()[0]
                    property = line.split()[1]
                    value = line.split()[2]
                    if property == "type":
                        if value == "filesystem":
                            dataset = Filesystem(name, self, self.dryrun)
                            self.datasets.add(dataset)
                        elif value == "volume":
                            dataset = Volume(name, self, self.dryrun)
                            self.datasets.add(dataset)
                        elif value == "snapshot":
                            if isinstance(dataset, Snapshot):
                                parent = dataset.dataset
                            else:
                                parent = dataset
                            dataset = Snapshot(name, parent, self.dryrun)
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

    def isScrubActive(self):
        result = ""
        parent = ""
        poollist = []
        for zpool in self.name.split("/"):
            poollist.append(parent + zpool)            
            parent += zpool + "/"

        #cmd = ["ps", "-A", "|", "grep", "-P", '"zpool scrub ('+"|".join(poollist)+')"', "|", "grep", "-v", "grep", "|", "wc", '-l']
        cmd = ["/sbin/zpool", "status", "|", "/bin/grep", "-e", '"scrub in progress"', "|", "wc", "-l"]     
        logging.debug(' '.join(cmd))
        result = int(check_output(' '.join(cmd), shell=True))
        logging.debug(result)
        return result > 0

    def getUsed(self):
        if self.dryrun:
            return self.__used
        else:
            return int(check_output(["/sbin/zfs", "get", "-H", "-p", "-o", "value", "used", self.name]))

    def setUsed(self, value):
        self.__used = value

    used = property(getUsed, setUsed)

    def getAvailable(self):
        if self.dryrun:
            return self.__available
        else:
            return int(check_output(["/sbin/zfs", "get", "-H", "-p", "-o", "value", "available", self.name]))

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

class Dataset(object):

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

    def getRemovableSnapshots(self):
        removableSnapshots = []
        for snapshot in self.snapshots:
            if not snapshot.keep:
                removableSnapshots.append(snapshot)
        return removableSnapshots

    removableSnapshots = property(getRemovableSnapshots)

    def getMaxRetention(self):
        if self.__maxRetention == None:
            if self.parent == None:
                return []
            else:
                return self.parent.maxRetention
        return self.__maxRetention

    def setMaxRetention(self, value):
        self.__maxRetention = value.split(" and ")

    maxRetention = property(getMaxRetention, setMaxRetention)

    def getRetentionPolicy(self):
        if self.__retentionPolicy == None:
            if self.parent == None:
                return []
            else:
                return self.parent.retentionPolicy
        return self.__retentionPolicy

    def setRetentionPolicy(self, value):
        self.__retentionPolicy = value.split(" and ")

    retentionPolicy = property(getRetentionPolicy, setRetentionPolicy)

    def getReferenced(self):
        if self.dryrun:
            return self.__referenced
        else:
            return int(check_output(["/sbin/zfs", "get", "-H", "-p", "-o", "value", "referenced", self.name]))

    def setReferenced(self, value):
        self.__referenced = value

    referenced = property(getReferenced, setReferenced)

    def destroySnapshotsOutOfMaxRetention(self):
        # Destroy snapshots out of maxRetention policy
        for snapshot in self.snapshots[:]:
            if snapshot.keep == False:
                cmd = ["/sbin/zfs", "destroy", "-d", snapshot.name]
                if self.dryrun:
                    logging.info(" ".join(cmd))
                else:
                    logging.debug(check_output(cmd))
                    logging.info("Snapshot '%s' has been destroyed" % snapshot.name)
                self.snapshots.remove(snapshot)

    def createBackupSnapshot(self):
        current = self.getCurrentBackupSnapshot()
        if current != None:
            return current
        
        snapshotname =  self.name+"@currentbackup"
        cmd = ["/sbin/zfs", "snapshot", snapshotname]
        if self.dryrun:
            logging.info(" ".join(cmd))
        else:
            logging.debug(check_output(cmd))
            logging.info("Snapshot '%s' has been created" % snapshotname)
        snapshot = Snapshot(snapshotname, self, self.dryrun)
        snapshot.creation = datetime.now()
        self.snapshots.append(snapshot)
        return snapshot

    def getMostRecentMatchingSnapshot(self, remotesnapshots):
        matchingSnap = None
        for snapshot in self.snapshots[:]:
            if snapshot.isLastBackup():
                return snapshot
            elif matchingSnap == None or matchingSnap.creation < snapshot.creation:
                for remotesnapshot in remotesnapshots[:]:
                    if remotesnapshot.creation == snapshot.creation:
                        matchingSnap = snapshot
        return matchingSnap

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


class Filesystem(Dataset):

    def getMaxFileAge(self):
        return self.__maxFileAge

    def setMaxFileAge(self, value):
        if [ m for m in [re.match('^(\d+) day[s]?$', value) ] if m != None ]:
            self.__maxFileAge = int(m.group(1))
        elif [ m for m in [ re.match('^(\d+) week[s]?$', value) ] if m != None ]:
            self.__maxFileAge = int(m.group(1)) * 7

    maxFileAge = property(getMaxFileAge, setMaxFileAge)

    def deleteFilesOverMaxFileAge(self):
        try:
            if self.maxFileAge != None:
                cmd = ["/usr/bin/find", "/%s" % (self.name), "-type", "f", "-ctime", "+%s" % (self.maxFileAge), "-print", "-delete"]
                if self.dryrun:
                    logging.info(" ".join(cmd))
                else:
                    for file in check_output(cmd).split("\n"):
                        if file != "":
                            logging.info("File '%s' has been deleted." % file)
                cmd = ["/usr/bin/find", "/%s" % (self.name), "-type", "d", "-ctime", "+%s" % (self.maxFileAge), "-mindepth", "2", "-empty", "-print", "-delete"]
                if self.dryrun:
                    logging.info(" ".join(cmd))
                else:
                    for directory in check_output(cmd).split("\n"):
                        if directory != "":
                            logging.info("Directory '%s' has been deleted." % directory)
        except AttributeError:
            pass

    def deleteOldestsFilesWhileNotUnderMaxCapacity(self):
        try:
            if self.maxCapacity != None:
                logging.debug("%s, %s" % (self.name, self.maxCapacity))
                for line in check_output(["/sbin/zfs", "get", "-H", "-p", "used,refquota,quota", self.name]).split("\n"):
                    if line != "":
                        name = line.split()[0]
                        property = line.split()[1]
                        value = line.split()[2]
                        if property == "used":
                            used = int(value)
                        elif property == "refquota" and int(value) > 0:
                            quota = int(value)
                        elif property == "quota" and int(value) > 0:
                            quota = int(value)
                logging.debug("quota = %s" % quota)
                logging.debug("used = %s" % used)
                logging.debug("threshold = %s" % (self.maxCapacity * quota))
                if used > self.maxCapacity * quota:
                    logging.debug("Over threshold")

                    for line in check_output("find /" + self.name + " -exec stat -f \"%m %z\" {} + | sort -n -k1", shell=True).split("\n"):
                        if line != "":
                            modificationTime, size = line.split(" ")
                            used -= int(size)
                            if used < self.maxCapacity * quota:
                                break
                    days = (datetime.today().date() - date.fromtimestamp(int(modificationTime))).days

                    cmd = ["/usr/bin/find", "/%s" % (self.name), "-type", "f", "-mtime", "+%s" % (days), "-print", "-delete"]
                    if self.dryrun:
                        logging.info(" ".join(cmd))
                    else:
                        for file in check_output(cmd).split("\n"):
                            if file != "":
                                logging.info("File '%s' has been deleted." % file)

                    cmd = ["/usr/bin/find", "/%s" % (self.name), "-type", "d", "-mtime", "+%s" % (days), "-mindepth", "2", "-empty", "-print", "-delete"]
                    if self.dryrun:
                        logging.info(" ".join(cmd))
                    else:
                        for directory in check_output(cmd).split("\n"):
                            if directory != "":
                                logging.info("Directory '%s' has been deleted." % directory)
                else:
                    logging.debug("Under threshold")
                
        except AttributeError:
            pass

class Volume(Dataset):

    pass

class Snapshot(object):

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
         return self.rename( self.name.replace('@currentbackup','@lastbackup'), True )

    def rename(self, name, force=False):
        # check if exists if forced
        if force:
            existingSnapshot = self.dataset.getSnapshot(name)
            if existingSnapshot != None:
                existingSnapshot.destroy()

        cmd = ["/sbin/zfs", "rename", self.name, name]
        if self.dryrun:            
            result = ''
            logging.info(" ".join(cmd))
        else:
            logging.debug(" ".join(cmd))
            result = check_output(cmd)
            logging.debug(result)
        if result=='':
            logging.info("Snapshot '%s' has been renamed to '%s'" % (self.name, name))
            self.name = name
        else:
            logging.error("Snapshot '%s' failed to be renamed to '%s'" % (self.name, name))        
        return result == ''

    def destroy(self):
        cmd = ["/sbin/zfs", "destroy", "-d", self.name]
        if self.dryrun:
            logging.info(" ".join(cmd))
            result = ''
        else:
            logging.debug(" ".join(cmd))
            result = check_output(cmd)
            logging.debug(result)
        if result == '' :
            logging.info("Snapshot '%s' has been destroyed" % self.name)
            self.dataset.snapshots.remove(self)
        else:
            logging.error("Snapshot '%s' failed to be destroyed" % self.name)
        return result==''
    
    def match(self, policy):
        # just for the record,
        #
        # if [ x for x in [ whatever() ] if cond(x) ] : process(x)
        # is a close Python transliteration of C's assign and test:
        #
        # if( cond(x=whatever()) ) process(x);
        # Pretty obscure, though.

        weekdays = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

        # all
        if [ m for m in [ re.match('^all$', policy) ] if m != None ]:
            logging.debug("Snapshot %s matches policy %s." % (self.name, policy))
            return True
        # none
        elif [ m for m in [ re.match('^none$', policy) ] if m != None ]:
            return False
        # n hour[s]
        elif [ m for m in [ re.match('^(\d+) hour[s]?$', policy) ] if m != None ]:
            if self.creation >= datetime.today() - timedelta(hours=int(m.group(1))):
                logging.debug("Snapshot %s matches policy %s." % (self.name, policy))
                return True
        # n day[s]
        elif [ m for m in [ re.match('^(\d+) day[s]?$', policy) ] if m != None ]:
            if self.creation.date() >= datetime.today().date() - timedelta(days=int(m.group(1))):
                logging.debug("Snapshot %s matches policy %s." % (self.name, policy))
                return True
        # n week[s]
        elif [ m for m in [ re.match('^(\d+) week[s]?$', policy) ] if m != None ]:
            if self.creation.date() >= datetime.today().date() - timedelta(weeks=int(m.group(1))):
                logging.debug("Snapshot %s matches policy %s." % (self.name, policy))
                return True
        # n (monday|tuesday|wednesday|thursday|friday|saturday|sunday)[s]
        elif [ m for m in [ re.match('^(\d+) (monday|tuesday|wednesday|thursday|friday|saturday|sunday)[s]?$', policy) ] if m != None ]:
            if weekdays[self.creation.weekday()] == m.group(2) and self.creation.date() >= datetime.today().date() - timedelta(weeks=int(m.group(1))):
                logging.debug("Snapshot %s matches policy %s." % (self.name, policy))
                return True
        # n n-th weekday of the month
        elif [ m for m in [ re.match('^(\d+) (\d+)(st|nd|rd|th) (monday|tuesday|wednesday|thursday|friday|saturday|sunday) of the month$', policy) ] if m != None ]:
            if int(self.creation.date().strftime("%d")) <= 7:
                weekdayofthemonth = 1
            elif int(self.creation.date().strftime("%d")) <= 14:
                weekdayofthemonth = 2
            elif int(self.creation.date().strftime("%d")) <= 21:
                weekdayofthemonth = 3
            elif int(self.creation.date().strftime("%d")) <= 28:
                weekdayofthemonth = 4
            else:
                weekdayofthemonth = 5
            if weekdays[self.creation.weekday()] == m.group(4) and weekdayofthemonth == int(m.group(2)):
                if int(datetime.today().date().strftime("%m")) == int(check_output(["date", "+%m", "--date", "last %s" % m.group(4)])):
                    date = MonthDelta(datetime.today().replace(day=1), int(m.group(1)) - 1)
                else:
                    date = MonthDelta(datetime.today().replace(day=1), int(m.group(1)))
                if self.creation.date() >= date.date():
                    logging.debug("Snapshot %s matches policy %s." % (self.name, policy))
                    return True
        # n n-th day of the month
        elif [ m for m in [ re.match('^(\d+) (\d+)(st|nd|rd|th) day of the month$', policy) ] if m != None ]:
            if int(self.creation.date().strftime("%d")) == int(m.group(2)) and self.creation.date() >= MonthDelta(datetime.today().replace(day=1), int(m.group(1)) - 1).date():
                logging.debug("Snapshot %s matches policy %s." % (self.name, policy))
                return True
        # n n-th day of the quarter
        elif [ m for m in [ re.match('^(\d+) (\d+)(st|nd|rd|th) day of the quarter$', policy) ] if m != None ]:
            if int(self.creation.date().strftime("%d")) == int(m.group(2)) and int(self.creation.date().strftime("%m")) % 3 == 1 and self.creation.date() >= MonthDelta(datetime.today().replace(day=1), int(m.group(1)) * 3).date():
                logging.debug("Snapshot %s matches policy %s." % (self.name, policy))
                return True
        # @snapshot
        elif [ m for m in [ re.match('^@([^ ]*)$', policy) ] if m != None ]:
            if self.name.split('@')[1] == m.group(1):
                logging.debug("Snapshot %s matches policy %s." % (self.name, policy))
                return True
        else:
            logging.critical("unknown policy: %s" % policy)
            sys.exit(1)
        return False

    def getKeep(self):

        # Default value is None : keep unless no more space (best effort)
        # if maxRetention is set value becomes False (mark as to be deleted), unless snapshot matches maxRetention then value becomes None
        # if snapshot matches retentionPolicy then value becomes True : always keep

        if not self.__keepTested:

            self.__keepTested = True

            if self.dataset.maxRetention != []:
                self.__keep = False
                for policy in self.dataset.maxRetention:
                    if self.match(policy):
                        logging.debug("Snapshot %s matches maxRetention policy %s, may keep it." % (self.name, policy))
                        self.__keep = None
                        if 'keep' in self.tags:
                            cmd = ["/sbin/zfs", "release", "keep", self.name]
                            if self.dryrun:
                                logging.info(" ".join(cmd))
                            else:
                                check_output(cmd)
                            self.__tags.remove('keep')
                            self.userrefs -= 1
                        break
                if self.__keep == False:
                    logging.debug("Snapshot %s does NOT match any maxRetention policy, must destroy it." % (self.name))
                    if 'keep' in self.tags:
                        cmd = ["/sbin/zfs", "release", "keep", self.name]
                        if self.dryrun:
                            logging.info(" ".join(cmd))
                        else:
                            check_output(cmd)
                        self.__tags.remove('keep')
                        self.userrefs -= 1
    
            for policy in self.dataset.retentionPolicy:
                if self.match(policy):
                    logging.debug("Snapshot %s matches retentionPolicy %s, have to keep it." % (self.name, policy))
                    self.__keep = True
                    if not 'keep' in self.tags:
                        cmd = ["/sbin/zfs", "hold", "keep", self.name]
                        if self.dryrun:
                            logging.info(" ".join(cmd))
                        else:
                            check_output(cmd)
                        self.__tags.add('keep')
                        self.userrefs += 1
                    break
    
            if self.__keep != True:
                if 'keep' in self.tags:
                    cmd = ["/sbin/zfs", "release", "keep", self.name]
                    if self.dryrun:
                        logging.info(" ".join(cmd))
                    else:
                        check_output(cmd)
                    self.__tags.remove('keep')
                    self.userrefs -= 1

        return self.__keep

    keep = property(getKeep)

    def getTags(self):
        if self.__tags == None:
            self.__tags = set()
            if self.userrefs > 0:
                for line in check_output(["/sbin/zfs", "holds", "-H", self.name]).split("\n"):
                    if line:
                        self.__tags.add(line.split()[1])
                        self.userrefs += 1
        return self.__tags

    tags = property(getTags)

