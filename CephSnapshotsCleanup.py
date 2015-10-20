#!/usr/local/bin/python
import logging, re
from datetime import timedelta
# dependency apt-get install python-dateutil
from dateutil.relativedelta import relativedelta


class CephSnapshotsCleanup(object):
	_clusterStats = None
	logLevel = logging.DEBUG
	
	def __init__(self, pool, image, policy=None, dryRun = False):
		if policy == None :
			policy = Config.get("POLICY", "time_to_live")
		
		self.policy = policy
		self._ttlcounts = { "h": 0, "d": 0, "w": 0, "m": 0, "y": 0, 'mandatory': 100}
		for ttl in re.split("\W+", policy):
			name = re.search("[a-zA-Z]",ttl)
			value = re.search("\d+",ttl)
			if name and value:
				self._ttlcounts[name.group(0)] = int(value.group(0))
		
		self.pool = pool
		self.dataset = pool.getDataset(image)
		self.image = image
		self.dryRun = dryRun
	
	
	
	def cleanAll(self):
		self._sortSnaps()
		for ttl,snaps in self._snaps.iteritems():
			pop = len(snaps) - self._ttlcounts[ttl]
			if pop > 0:
				self._trash.append(snaps.pop(-1 * pop) )
		
		if self.logLevel <= logging.DEBUG :
			logging.debug( "Snaps kept for policy %s : " % self.policy )
			for ttl,snaps in self._snaps.iteritems():
				logging.debug( "Type : %s" % ttl)
				for s in snaps:
					logging.debug( s.name )
				
		logging.debug("Snaps deleted: ")
		for s in self._trash:
			s.destroy()
	
	def _sortSnaps(self):
		self._snaps = { 'h': [], 'd': [], 'w': [], 'm': [], 'y': [], 'mandatory': [] };
		self._trash = []
		#dataset.snapshots sorted latest first: -1 oldest, 0 most recent
		for snap in self.dataset.snapshots :
			# keep current and last ones for increment
			if snap.isLastBackup() or snap.isCurrentBackup():
				self._snaps["mandatory"].append(snap)
				continue
		
			# not same hour and same day
			if ( 
					len(self._snaps['h']) == 0 or 
					(
						snap.creation <= self._snaps['h'][-1].creation + relativedelta(hours=-1) and
						snap.creation >= self._snaps['h'][0].creation + relativedelta(hour=0,minute=0,second=0) 
					)
				):
				self._snaps["h"].append(snap)
				continue
			logging.debug( "Not for h : %s <= %s and >= %s" % (snap.creation, self._snaps['h'][-1].creation + relativedelta(hours=-1), self._snaps['h'][0].creation + relativedelta(hour=0,minute=0,second=0)))
			
			# not same day and last 31 days only
			if ( 
					(
						len(self._snaps['d']) == 0 and
						len(self._snaps['h']) > 0 and
						snap.creation <= self._snaps['h'][0].creation + relativedelta(hour=0,minute=0,second=0)
					)
					or 
					(
						len(self._snaps['d']) > 0 and
						snap.creation < self._snaps['d'][-1].creation + relativedelta(hour=0,minute=0,second=0) and
						snap.creation >= self._snaps['d'][0].creation + relativedelta(days=-31, hour=0,minute=0,second=0) 
					)
				):
				self._snaps['d'].append(snap)
				continue
			logging.debug( "Not for d : %s <= %s and >= %s" % (snap.creation, self._snaps['d'][-1].creation + relativedelta(hour=0,minute=0,second=0), self._snaps['d'][0].creation + relativedelta(days=-31, hour=0,minute=0,second=0)))
				
			# not same week and last 52 weeks only
			if ( 
					(
						len(self._snaps['w']) == 0 and
						len(self._snaps['d']) > 0 and
						snap.creation <= self._snaps['d'][0].creation + relativedelta(weekday=0,hour=0,minute=0,second=0)
					)
					or 
					(
						len(self._snaps['w']) > 0 and
						snap.creation < self._snaps['w'][-1].creation + relativedelta(weekday=0,hour=0,minute=0,second=0) and
						snap.creation >= self._snaps['w'][0].creation + relativedelta(weeks=-52, weekday=0,hour=0,minute=0,second=0) 
					)
				):
				self._snaps['w'].append(snap)
				continue
				
			# not same month and last 12 months only
			if ( 
					(
						len(self._snaps['m']) == 0 and
						len(self._snaps['w']) > 0 and
						snap.creation <= self._snaps['w'][0].creation + relativedelta(day=1,hour=0,minute=0,second=0)
					)
					or 
					(
						len(self._snaps['m']) > 0 and
						snap.creation < self._snaps['m'][-1].creation + relativedelta(day=1,hour=0,minute=0,second=0) and
						snap.creation >= self._snaps['m'][0].creation + relativedelta(days=-365, day=1,hour=0,minute=0,second=0)
					)
				):
				self._snaps['m'].append(snap)
				continue
				
			# not same year
			if ( 
					(
						len(self._snaps['y']) == 0 and
						len(self._snaps['m']) > 0 and
						snap.creation <= self._snaps['m'][0].creation + relativedelta(month=1,day=1,hour=0,minute=0,second=0)
					)
					or 
					(
						len(self._snaps['y']) > 0 and
						snap.creation < self._snaps['y'][-1].creation + timedelta(month=1,day=1,hour=0,minute=0,second=0)
					)
				):
				self._snaps['y'].append(snap)
				continue
			
			self._trash.append(snap)
			
