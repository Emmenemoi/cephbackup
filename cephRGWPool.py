#!/usr/local/bin/python

import sys, getopt, re, fcntl, os
import logging
from xml.dom.minidom import parse
from subprocess import Popen, PIPE, check_output, CalledProcessError, STDOUT
from datetime import datetime, timedelta, date
from cepherror import *
try:
	import rados
except ImportError:
	rados = None
	

class CephRGWPool(object):
	_bases = ['.rgw.root', '.rgw.control', '.rgw.gc', '.rgw.buckets', '.rgw.buckets.index', '.rgw.buckets.extra', '.log', '.intent-log', '.usage', '.users', '.users.email', '.users.swift', '.users.uid']
	
	def __init__(self, geography, conf, user, keyring, dryrun=True):
		self.pools = []
		self._prefix = ''
		if (geography != 'default':
			self._prefix = '.' + geography
		try:
			for pool in CephRGWPool._bases:
				self.pools.append( CephPool(self._prefix + pool, conf, user, keyring, dryrun) )
		except Error:
			logging.error("Impossible to load pools for geography %s" % (geography) )
			
		