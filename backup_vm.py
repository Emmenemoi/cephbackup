#!/usr/local/bin/python

import subprocess, time, re, logging

def toggleVMState(xapi_session, name, toPause=True):
	vm_ref = next(iter(xapi_session.xenapi.VM.get_by_name_label(name) or []), None)
	if vm_ref is not None:
		#record = xapi_session.xenapi.VM.get_record(vm_ref)
		#logging.info( "Details of vm-100 : %s" % record)
		power = xapi_session.xenapi.VM.get_power_state(vm_ref)
		logging.debug( "Existing powerstate of %s : %s" % (name, power) )
		if power == "Running" and toPause:
			xapi_session.xenapi.VM.pause(vm_ref)
		if power == "Paused" and not toPause:
			xapi_session.xenapi.VM.unpause(vm_ref)
		power = xapi_session.xenapi.VM.get_power_state(vm_ref)
		logging.info( "New powerstate of %s : %s" % (name, power) )
	else:
		logging.info( "VM not recognised  : %s" % (name) )
	


def backup_vm( image_name , xapi_session = None):
	data = re.split('-', image_name)
	if ( len(data) > 1 ):
		vmid = data[1]
	else:
		vmid = data[0]

	# image_name = 'vm-'+vmid
	sourceDataset = backup_vm.sourcePool.getDataset( image_name )
	backupDataset = backup_vm.backupPool.getDatasetOrCreate( image_name )

	lastBackupIncrementSnapshot = None
	lastSourceIncrementSnapshot = None
	if sourceDataset != None :
		lastSourceIncrementSnapshot = sourceDataset.getLastBackupSnapshot()
		# do some cleaning if last run failed
		currentSourceSnapshot = sourceDataset.getCurrentBackupSnapshot()
		if currentSourceSnapshot != None:
			if not currentSourceSnapshot.renameToLastBackup():
				sys.exit(2)

		# be sure it exists or maybe we could find another old one
		if backupDataset != None and ( lastSourceIncrementSnapshot == None or backupDataset.getSnapshot( lastSourceIncrementSnapshot.name ) == None ) :
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
		# be sure it exists or maybe we could find another old one
		if sourceDataset != None and ( lastBackupIncrementSnapshot == None or sourceDataset.getSnapshot( lastBackupIncrementSnapshot.name ) == None ) :
			lastBackupIncrementSnapshot = sourceDataset.getMostRecentMatchingSnapshot( backupDataset.snapshots )
	else:
		logging.error("Impossible to find backup dataset for VM %s" % (vmid) )

	if xapi_session is not None:
		toggleVMState(xapi_session, image_name)

	newsnapshot = sourceDataset.createBackupSnapshot()

	if xapi_session is not None:
		toggleVMState(xapi_session, image_name, False)

	
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
		backupDataset = backup_vm.backupPool.getDataset( image_name )
		lastBackupSnapshot = None
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
