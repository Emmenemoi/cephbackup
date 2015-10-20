#!/usr/local/bin/python

import sys, getopt, re, fcntl, os
import logging
from xml.dom.minidom import parse
from subprocess import Popen, PIPE, check_output, CalledProcessError, STDOUT
from datetime import datetime, timedelta, date
from CephError import *
try:
	import rados
	import rbd
except ImportError:
	rados = None
	rbd = None
	

class CephPool(object):
	_clusterStats = None
	
	def __init__(self, name, conf, user, keyring, dryrun=True):
		self.name = name
		self.dryrun = dryrun
		self.maxCapacity = 0.8
		self.bestEffortPolicy = "morerem"
		self.cephRbdArgs = ['-c', conf, '--id', user]
		if keyring != None:
			self.cephRbdArgs.extend(['--keyring', keyring])
		self._conf = conf
		self._user=user
		self._keyring=keyring
		logging.info("Loading rbd config at %s" % (conf))
		config = dict()
		if (keyring != None):
			config["keyring"] = keyring
		self._client = rados.Rados(conffile=conf, rados_id=user, conf=config)
		try:
			self._client.connect()
			try:
				self.ioctx = self._client.open_ioctx(self.name)
				self.rbd = rbd.RBD()
			except rados.Error:
				# shutdown cannot raise an exception
				self._client.shutdown()
				raise CephError(self, 'Cannot connect to Pool')
				
			if self.isScrubActive(): 
				raise CephError(self, 'Pool is scrubbing')
			else:
				self.__used = self.getClusterStats()["kb_used"]
				self.__available = self.getClusterStats()["kb_avail"]
				self.refreshDatasets()
				
		except rados.Error:
			raise CephError(self, 'Pool Exception for %s' % (self.name))

	def __exit__(self, exc_type, exc_value, traceback):
		self._disconnect_from_rados()
	
	def _disconnect_from_rados(self):
		"""Terminate connection with the Ceph cluster."""
		# closing an ioctx cannot raise an exception
		self.ioctx.close()
		self._client.shutdown()

	def refreshDatasets(self):
		self.datasets = set()
		logging.info("Getting rbd volumes information for pool %s" % (self.name))
		for image in self.rbd.list(self.ioctx):
			dataset = Dataset(image, self, self.dryrun)
			self.datasets.add(dataset)
				
	def isScrubActive(self):
		return False
		#result = ""
		#parent = ""
		#poollist = []
		#for pool in self.name.split("/"):
		#	poollist.append(parent + pool)			
		#	parent += pool + "/"
		#cmd = ["ps", "-A", "|", "grep", "-P", '"zpool scrub ('+"|".join(poollist)+')"', "|", "grep", "-v", "grep", "|", "wc", '-l']
		#cmd = ["/sbin/zpool", "status", "|", "/bin/grep", "-e", '"scrub in progress"', "|", "wc", "-l"]	 
		#logging.debug(' '.join(cmd))
		#result = int(check_output(' '.join(cmd), shell=True))
		#logging.debug(result)
		#return result > 0

	def getUsed(self):
		if self.dryrun:
			return self.__used
		else:
			return self.getClusterStats().kb_used

	def getClusterStats(self):
		if CephPool._clusterStats == None or self.dryrun:
			CephPool._clusterStats = self._client.get_cluster_stats()
		return CephPool._clusterStats

	def setUsed(self, value):
		self.__used = value

	used = property(getUsed, setUsed)

	def getAvailable(self):
		if self.dryrun:
			return self.__available
		else:
			return self.getClusterStats().kb_avail

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

	def getDatasetOrEmpty(self, name):
		dataset = self.getDataset(name)
		if dataset == None:
			dataset = Dataset(name, self, self.dryrun, False)
			self.datasets.add(dataset)
			
		return dataset

	def getDatasetOrCreate(self, name):
		dataset = self.getDataset(name)
		if dataset == None:
			logging.info("Create Image %s on pool %s" % (name, self.name))
			self.rbd.create(self.ioctx, name, 10)
			dataset = Dataset(name, self, self.dryrun)
			self.datasets.add(dataset)
			
		return dataset
		
	def getReferenced(self):
		referenced = 0
		for dataset in self.datasets:
			referenced += dataset.referenced
		return referenced

	referenced = property(getReferenced)

class Dataset(object):
	snapshotPattern = 'backup%Y-%m-%dT%H.%M.%S'
	today = datetime.now()
	
	def __init__(self, name, pool, dryrun=True, exists=True):
		self.name = name
		self.pool = pool
		self.dryrun = dryrun
		self.snapshots = []
		self.__maxRetention = None
		self.__retentionPolicy = None
		self._exists = exists
		self.userrefs = None
		self._rbdImage = None
		if exists:
			self._rbdImage = rbd.Image(self.pool.ioctx, name)
			self.stats = self._rbdImage.stat()
			if name.count('/') > 0:
				self.parent = pool.getDataset(name.rsplit('/', 1)[0])
			else:
				self.parent = None
			for snap in self._rbdImage.list_snaps():
				snapshot = Snapshot(snap['id'], snap['name'], self, self.dryrun)
				snapshot.used = snap['size']
				self.snapshots.append(snapshot)
			self.sortSnaps()
			for s in self.snapshots:
				logging.debug("%s/%s (%s)" % (self.name, s.name, s.creation))

	def __del__(self):
		"""Delete Dataset."""
		if self._exists and self._rbdImage != None:
			self._rbdImage.close()
		
	def __exit__(self, exc_type, exc_value, traceback):
		"""Close Dataset."""
		if self._exists and self._rbdImage != None:
			self._rbdImage.close()

	def sortSnaps(self):
		self.snapshots = sorted(self.snapshots, key=lambda snapshot: snapshot.creation, reverse=True) # sorted latest first
	
	def getRemovableSnapshots(self):
		removableSnapshots = []
		for snapshot in self.snapshots:
			if not snapshot.keep:
				removableSnapshots.append(snapshot)
		return removableSnapshots

	removableSnapshots = property(getRemovableSnapshots)

	def destroySnapshotsOutOfMaxRetention(self):
		# Destroy snapshots out of maxRetention policy
		for snapshot in self.snapshots[:]:
			if snapshot.keep == False:				
				if self.dryrun:
					logging.info("Image.remove_snap("+snapshot.name+")")
				else:
					self._rbdImage.remove_snap(snapshot.name)
					logging.info("Snapshot '%s/%s@%s' has been destroyed" % (self.pool.name, self.name, snapshot.name))
				self.snapshots.remove(snapshot)

	def createBackupSnapshot(self):
		# impossible to rename for the moment, so we cannot flag and then restart former failed backup
		#current = self.getCurrentBackupSnapshot()
		#if current != None:
		#	return current

		snapshotname =  Dataset.today.strftime(Dataset.snapshotPattern)+""
		if self.dryrun:
			logging.info("Image.create_snap("+snapshotname+")")
		else:
			self._rbdImage.create_snap(snapshotname)
			logging.info("Snapshot '%s' has been created" % snapshotname)
		snapshot = Snapshot(None, snapshotname, self, self.dryrun)
		self.snapshots.append(snapshot)
		self.sortSnaps()
		return snapshot

	def getMostRecentMatchingSnapshot(self, remotesnapshots):
		matchingSnap = None
		for snapshot in self.snapshots[:]:
			#if snapshot.isLastBackup():
			#	return snapshot
			#elif
			if matchingSnap == None or matchingSnap.creation < snapshot.creation:
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
		
	def rollBackupNames(self):
		lastBackup = self.getLastBackupSnapshot()
		if lastBackup != None:
			lastBackup.rename( lastBackup.name.replace('L','') )
	
		for snapshot in self.snapshots[:]:
			if snapshot.isCurrentBackup():
				snapshot.renameToLastBackup()
	
	# no SSH connection so it doesn't matter export / import, ie: initiating node.
	def exportSnapshot(self, remoteDataset, localsnapshot, incrementalSnap=None):
		logging.debug("Performing differential transfer from '%(src)s' to '%(dest)s'", {'src': self.name, 'dest': remoteDataset.name})
		cmd1 = ['rbd']
		cmd1.extend(self.pool.cephRbdArgs )
		cmd1.extend(['export-diff' ])
		
		if incrementalSnap != None:
			cmd1.extend(['--from-snap', incrementalSnap.name])
		
		path = "%s/%s@%s" % (self.pool.name, self.name, localsnapshot.name)
		cmd1.extend([path, '-'])
		
		cmd2 = ['rbd']
		cmd2.extend(remoteDataset.pool.cephRbdArgs )
		cmd2.extend(['import-diff' ])
		rbd_path = "%s/%s" % (remoteDataset.pool.name, remoteDataset.name)
		cmd2.extend(['-', rbd_path])
			
		if self.dryrun:
			logging.info(" ".join(cmd1) + ' | ' + " ".join(cmd2))
			result = None
			stderr = ''
		else:
			result, stderr = self._piped_execute(cmd1, cmd2)
			if result:
				msg = "RBD diff op failed - (ret=%(ret)s stderr=%(stderr)s)" % {'ret': result, 'stderr': stderr}
				raise CephError(self.pool,msg)
				
		if 'already exists' in stderr:
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
				return self.exportSnapshot(remoteDataset, localsnapshot, lastIncrementSnapshot)
			else:
				return self.exportSnapshot(remoteDataset, localsnapshot)
		elif not result:
			logging.info("Snapshot '%s' has been exported to %s" % (localsnapshot.name, self.name))
		else:
			logging.error("Snapshot '%s' failed to be exported to %s" % (localsnapshot.name, self.name))

		if not result:
			remoteDataset.pool.refreshDatasets()
		return not result
	
	def _piped_execute(self, cmd1, cmd2):
		"""Pipe output of cmd1 into cmd2."""
		logging.debug("Piping cmd1='%s' into...", ' '.join(cmd1))
		logging.debug("cmd2='%s'", ' '.join(cmd2))

		try:
			p1 = Popen(cmd1, stdout=PIPE,
								  stderr=PIPE)
		except OSError as e:
			logging.error(_LE("Pipe1 failed - %s "), e)
			raise

		# NOTE(dosaboy): ensure that the pipe is blocking. This is to work
		# around the case where evenlet.green.subprocess is used which seems to
		# use a non-blocking pipe.
		flags = fcntl.fcntl(p1.stdout, fcntl.F_GETFL) & (~os.O_NONBLOCK)
		fcntl.fcntl(p1.stdout, fcntl.F_SETFL, flags)

		try:
			p2 = Popen(cmd2, stdin=p1.stdout,
								  stdout=PIPE,
								  stderr=PIPE)
		except OSError as e:
			logging.error(_LE("Pipe2 failed - %s "), e)
			raise

		p1.stdout.close()
		stdout, stderr = p2.communicate()
		return p2.returncode, stderr
		

class Volume(Dataset):
	pass

class Snapshot(object):
	snapshotPattern = "^backup\d{4}-\d{2}-\d{2}T\d{2}\.\d{2}\.\d{2}$"
	currentPattern = re.compile("^backup\d{4}-\d{2}-\d{2}T\d{2}\.\d{2}\.\d{2}C$")
	lastPattern = re.compile("^backup\d{4}-\d{2}-\d{2}T\d{2}\.\d{2}\.\d{2}L$")

	def __init__(self, id, name, dataset, dryrun=True):
		self.id = id
		self.name = name
		self.dataset = dataset
		self.dryrun = dryrun
		self.__keep = None
		self.__keepTested = False
		self.__tags = None
		self.creation = None
		self.used = 0
		self.isCurrent = False
		self.isLast = False
		try:
			snaptime = re.search("("+Snapshot.snapshotPattern+")", name).group(1)
			self.creation = datetime.strptime(snaptime, Dataset.snapshotPattern )
			#logging.debug("%s creation time %s" % (name, self.creation))
		except AttributeError:
			logging.info("Cannot determine %s creation time for pattern %s" % (name,Snapshot.snapshotPattern))
					
		
	def __del__(self):
		if self.dryrun:
			self.dataset.pool.used -= self.used
			self.dataset.pool.available += self.used

	def isLastBackup(self):
		#return Snapshot.lastPattern.match(self.name)
		return len(self.dataset.snapshots) >= 2 and self.name == self.dataset.snapshots[1].name

	def isCurrentBackup(self):
		#return Snapshot.currentPattern.match(self.name)
		return len(self.dataset.snapshots) >= 1 and self.name == self.dataset.snapshots[0].name

	def renameToLastBackup(self):
		# no rename for the moment, so no C and L
		return self.rename( self.name.replace('C','L'), True )

	def rename(self, name, force=False):
		return True
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
		try:
			if self.dryrun:
				logging.info("Image.remove_snap("+self.name+")")
				result = ''
			else:
				self.dataset._rbdImage.remove_snap(self.name)

			logging.info("Snapshot '%s' has been destroyed" % self.name)
			self.dataset.snapshots.remove(self)
			return True
		except rados.Error:
			logging.error("Snapshot '%s' failed to be destroyed" % self.name)
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
			if self.dataset._rbdImage.is_protected_snap(self.name):
				self.__tags.add('keep')
		return self.__tags

	tags = property(getTags)

