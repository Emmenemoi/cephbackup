#!/usr/bin/python

#
#   Config file cephbackup.conf should contain:
#
#[MAIN]
#source_ceph_conf = 
#backup_ceph_conf = 
#source_ceph_pool = rbd
#backup_ceph_pool = rbdbackup
#backup_ceph_user = backup 
#source_ceph_user = admin 
#backup_ceph_keyring = /etc/ceph/<stdkeyring name>
#source_ceph_keyring = /etc/ceph/<stdkeyring name>
#
#[VMLIST]
#backups = <space separated xen machines>
#

import subprocess, time, re, ConfigParser, logging, sys, os, getopt, fcntl
from CephPool import *

pid_file = '/var/run/cephlivebackup.pid'
logfile = "/var/log/cephbackup/backup.log"
configfile = "/etc/cephbackup.conf"

silent = False
dryrun = False

class StreamToLogger(object):
   """
   Fake file-like stream object that redirects writes to a logger instance.
   """
   def __init__(self, logger, log_level=logging.INFO):
      self.logger = logger
      self.log_level = log_level
      self.linebuf = ''
 
   def write(self, buf):
      for line in buf.rstrip().splitlines():
         self.logger.log(self.log_level, line.rstrip())


def get_local_backup_vms():
   result = []

   for dataset in backup_vm.sourcePool.datasets:
      logging.info("Check if %s should be backuped" % (dataset.name))
      #data = re.split('[\s]+', dataset.name)
      #uuid = data[1]
      #name = data[0]
      if ( dataset.name in livebackups ) :
          result += [dataset.name]
   
   return result

def backup_vm( vmhostname ):
	vmid = data = re.split('\.', vmhostname)[0]

	sourceDataset = backup_vm.sourcePool.getDataset(vmid+'.vm')
	backupDataset = backup_vm.backupPool.getDatasetOrCreate(vmid+'.vm')
    
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
	else:
		logging.error("Cannot import: mmight need to clean old snapshots.")


# be sure runs only once
fp = open(pid_file, 'w')
try:
    fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
except IOError:
    # another instance is running
    sys.exit(0)

try:
  opts, args = getopt.getopt( sys.argv[1:] ,"shd",["silent", "dry-run", "config-file="])
except getopt.GetoptError:
  print 'usage: -s or --silent / -d or --dry-run / --config-file=<path>'
  sys.exit(2)

for opt, arg in opts:
      if opt == '-h':
         print ' -s: silent'
         sys.exit()
      elif opt in ("-s", "--silent"):
         silent = True
      elif opt in ("-d", "--dry-run"):
         dryrun = True
      elif opt in ("--config-file"):
	configfile = arg

if (silent) :
    # verify arancloud log
    if not os.path.exists(os.path.dirname(logfile)):
        os.makedirs(os.path.dirname(logfile))
    logging.basicConfig(
       level=logging.DEBUG,
       format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
       filename=logfile,
       filemode='a'
    )

    stdout_logger = logging.getLogger('STDOUT')
    slo = StreamToLogger(stdout_logger, logging.INFO)
    sys.stdout = slo
     
    stderr_logger = logging.getLogger('STDERR')
    sle = StreamToLogger(stderr_logger, logging.ERROR)
    sys.stderr = sle
else:
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)

Config = ConfigParser.SafeConfigParser({'source_ceph_conf': '/etc/ceph/ceph.conf', 'backup_ceph_conf':'/etc/ceph/ceph.backup.conf' , 'source_ceph_user': 'admin', 'backup_ceph_user': 'backup', 'source_ceph_pool': 'rbd', 'backup_ceph_pool': 'rbdbackup', 'source_ceph_keyring': None, 'backup_ceph_keyring': None })
config_candidates = [configfile]
Config.read( config_candidates )
livebackups = re.split('[\s]+', Config.get("VMLIST", "backups") )
source_ceph_conf = Config.get("MAIN", "source_ceph_conf")
backup_ceph_conf = Config.get("MAIN", "backup_ceph_conf")
source_ceph_pool = Config.get("MAIN", "source_ceph_pool" )
backup_ceph_pool = Config.get("MAIN", "backup_ceph_pool" )
source_ceph_user = Config.get("MAIN", "source_ceph_user")
backup_ceph_user = Config.get("MAIN", "backup_ceph_user")
source_ceph_keyring = Config.get("MAIN", "source_ceph_keyring")
backup_ceph_keyring = Config.get("MAIN", "backup_ceph_keyring")

try:
    backup_vm.backupPool = CephPool(backup_ceph_pool, backup_ceph_conf, backup_ceph_user, backup_ceph_keyring, dryrun)
    backup_vm.sourcePool = CephPool(source_ceph_pool, source_ceph_conf, source_ceph_user, source_ceph_keyring, dryrun)

    for (name) in get_local_backup_vms():
       timestamp = time.strftime("%Y%m%d-%H:%M", time.gmtime())
       #print timestamp, uuid, name
       backup_vm( name )
except CephError, e:
  print e
  sys.exit(2)


