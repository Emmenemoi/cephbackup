#!/usr/local/bin/python

import subprocess, time, re, logging

def backup_radosgw( sourcePool, backupPool ):
    
	lastBackupIncrementSnapshot = None
	lastSourceIncrementSnapshot = None
	if sourceDataset != None :
		lastSourceIncrementSnapshot = sourceDataset.getLastBackupSnapshot()
		# do some cleaning if last run failed
		currentSourceSnapshot = sourceDataset.getCurrentBackupSnapshot()
		if currentSourceSnapshot != None:
			if not currentSourceSnapshot.renameToLastBackup():
				sys.exit(2)

		if lastSourceIncrementSnapshot == None and backupDataset != None:
			lastSourceIncrementSnapshot = backupDataset.getMostRecentMatchingSnapshot( sourceDataset.snapshots )
	else:
		logging.error("Impossible to find source dataset for VM %s" % (vmid) )

	if backupDataset != None:
		# do some cleaning if last failed
		currentBackupSnapshot = backupDataset.getCurrentBackupSnapshot()
		if currentBackupSnapshot != None:
			if not currentBackupSnapshot.renameToLastBackup():
				sys.exit(2)

		lastBackupIncrementSnapshot = backupDataset.getLastBackupSnapshot()
		# maybe we could find another old one 
		if lastBackupIncrementSnapshot == None and sourceDataset != None:
			lastBackupIncrementSnapshot = sourceDataset.getMostRecentMatchingSnapshot( backupDataset.snapshots )
	else:
		logging.error("Impossible to find backup dataset for VM %s" % (vmid) )

	newsnapshot = sourceDataset.createBackupSnapshot()
	if lastSourceIncrementSnapshot != None and lastBackupIncrementSnapshot != None:
		# incremental send possible
		success = sourceDataset.exportSnapshot(backupDataset, newsnapshot, lastSourceIncrementSnapshot)            
	else:
		# we create a new fresh send
		success = sourceDataset.exportSnapshot(backupDataset, newsnapshot)

	if success:
		#if lastLocalIncrementSnapshot != None:
		#    lastLocalIncrementSnapshot.destroy()
		newsnapshot.renameToLastBackup()
		backupDataset = backup_vm.backupPool.getDataset(vmid +'.vm')
		if backupDataset != None:
			backupDataset.rollBackupNames()
		# keep only last snapshot available for later increment
		lastBackupSnapshot = sourceDataset.getMostRecentMatchingSnapshot( backupDataset.snapshots )
		if lastBackupSnapshot != None:
			# lastBackupSnapshot exists on both sides for later increment: delete others (olders)
			logging.info("cleaning dataset %s from pool %s, keep %s" % (sourceDataset.name, sourceDataset.pool.name, lastBackupSnapshot.name) )
			destroylist = [snap for snap in sourceDataset.snapshots if snap.name != lastBackupSnapshot.name and snap.creation != lastBackupSnapshot.creation ]
			for snap in destroylist:
				snap.destroy()
	else:
		logging.error("Cannot import: might need to clean old snapshots.")
