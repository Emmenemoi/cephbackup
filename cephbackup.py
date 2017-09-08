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
#xenserver_master = 
#xenserver_user = 
#xenserver_password = 
#
#[VMLIST]
#<space separated xen machines>
#backups =
#
#[RBDLIST]
#<space separated RBD names>
#backups =
#
#[RADOSGW]
#geographies = default
#
#[POLICY]
## h: 1 every hour, d: 1 every day, w: 1 every week, m: 1 every month, y: 1 every year
#time_to_live = 30d,4w,12m,1y
#

import subprocess, time, re, ConfigParser, logging, sys, os, getopt, fcntl
from CephPool import *
from CephSnapshotsCleanup import *
from backup_vm import *

## Xenserver compat for atomic snapshots
import XenAPI

pid_file = '/var/run/cephlivebackup.pid'
logfile = "/var/log/cephbackup/backup.log"
configfile = "/etc/cephbackup.conf"

silent = False
dryrun = False
cleanOnly = False
loggingLevel = logging.INFO

RBDPOOL_PREFIX = "RBD_XenStorage-"
VDI_PREFIX = "VHD-"

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
      if ( dataset.name in livebackups or dataset.name.replace('vm-','') in livebackups ) :
          result += [dataset.name]

   return result


		
try:
  opts, args = getopt.getopt( sys.argv[1:] ,"shdcv",["silent", "dry-run", "config-file=", "pid-file=", "log-file=", "clean-only", "verbose"])
except getopt.GetoptError:
  print 'usage: -s or --silent / -d or --dry-run / --config-file <path> / --pid-file <path> / --log-file <path> / -v or --verbose'
  sys.exit(2)

for opt, arg in opts:
	if opt == '-h':
		print ' -s: silent'
		sys.exit()
	elif opt in ("-s", "--silent"):
		silent = True
	elif opt in ("-d", "--dry-run"):
		dryrun = True
	elif opt == "--config-file":
		configfile = arg
	elif opt == "--pid-file":
		pid_file = arg
	elif opt == "--log-file":
		logfile = arg
	elif opt in ("-c", "--clean-only"):
		cleanOnly = True
	elif opt in ("-v", "--verbose"):
		loggingLevel = logging.DEBUG


if (silent) :
    # verify arancloud log
    if not os.path.exists(os.path.dirname(logfile)):
        os.makedirs(os.path.dirname(logfile))
    logging.basicConfig(
       level=loggingLevel,
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
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=loggingLevel)


# be sure runs only once
fp = open(pid_file, 'w')
try:
    fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
except IOError:
    # another instance is running
    sys.exit(0)


Config = ConfigParser.SafeConfigParser({'source_ceph_conf': '/etc/ceph/ceph.conf', 'backup_ceph_conf':'/etc/ceph/ceph.backup.conf' , 'source_ceph_user': 'admin', 'backup_ceph_user': 'backup', 'source_ceph_pool': 'rbd', 'backup_ceph_pool': 'rbdbackup', 'source_ceph_keyring': None, 'backup_ceph_keyring': None, 'xenserver_master':None, 'xenserver_user':None, 'xenserver_password':None, 'time_to_live': '30d,4w,12m,1y' })
configCandidates = [configfile]
found = Config.read( configCandidates )
missing = set(configCandidates) - set(found)
logging.info('Found config files: %s' % sorted(found))
logging.debug('Missing files     : %s'% sorted(missing))

livebackups = []
if Config.has_section("VMLIST"):
	livebackups += re.split('[\s]+', Config.get("VMLIST", "backups") )
if Config.has_section("RBDLIST"):
	livebackups += re.split('[\s]+', Config.get("RBDLIST", "backups") )

source_ceph_conf = Config.get("MAIN", "source_ceph_conf")
backup_ceph_conf = Config.get("MAIN", "backup_ceph_conf")
source_ceph_pool = Config.get("MAIN", "source_ceph_pool" )
backup_ceph_pool = Config.get("MAIN", "backup_ceph_pool" )
source_ceph_user = Config.get("MAIN", "source_ceph_user")
backup_ceph_user = Config.get("MAIN", "backup_ceph_user")
source_ceph_keyring = Config.get("MAIN", "source_ceph_keyring")
backup_ceph_keyring = Config.get("MAIN", "backup_ceph_keyring")

xenserver_master_host = Config.get("MAIN", "xenserver_master")
xenserver_user = Config.get("MAIN", "xenserver_user")
xenserver_pwd = Config.get("MAIN", "xenserver_password")

policy = Config.get("POLICY", "time_to_live")

xapi_session = None
if xenserver_master_host is not None:
	xapi_session = XenAPI.Session(xenserver_master_host, ignore_ssl=True)
	try:
		xapi_session.xenapi.login_with_password(xenserver_user, xenserver_pwd, "1.0", "cephbackup.py")
	except XenAPI.Failure as f:
		logging.error( "Failed to acquire a session: %s" % f.details)
		sys.exit(1)

try:
	backup_vm.backupPool = CephPool(backup_ceph_pool, backup_ceph_conf, backup_ceph_user, backup_ceph_keyring, dryrun)
	backup_vm.sourcePool = CephPool(source_ceph_pool, source_ceph_conf, source_ceph_user, source_ceph_keyring, dryrun)

	CephSnapshotsCleanup.logLevel = loggingLevel
	for (name) in get_local_backup_vms():
		timestamp = time.strftime("%Y%m%d-%H:%M", time.gmtime())
		#print timestamp, uuid, name
		if cleanOnly == False:
			backup_vm( name, xapi_session )
		cleaner = CephSnapshotsCleanup(backup_vm.backupPool, name, policy, dryrun)
		cleaner.cleanAll()

	if Config.has_section("RADOSGW"):
	    rgw_geo = Config.get("RADOSGW", "geographies")
	    if rgw_geo != None:
		    rgwbackups = re.split('[\s]+', rgw_geo)
		    source = CephRGWPool(rgwbackups, backup_ceph_conf, backup_ceph_user, backup_ceph_keyring, dryrun)
		    backup = CephRGWPool(rgwbackups, backup_ceph_conf, backup_ceph_user, backup_ceph_keyring, dryrun)
		    backup_radosgw(source, backup)

except CephError, e:
  print e
  sys.exit(2)

finally:
	xapi_session.xenapi.session.logout()

