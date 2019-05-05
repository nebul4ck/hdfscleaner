# -*- encoding: utf-8 -*-

"""
.. module:: main
   :platform: GNU/Linux
   :synopsis: Clean Hadoop old files.
.. moduleauthor::
   :Nickname: Alberto González
   :mail: agonzalez@wtelecom.es
   :Web :	https://nebul4ck.wordpress.com/
"""

from hdfs import InsecureClient
from hdfscleaner.conf.sparkConf import *
from hdfscleaner.lib.logger import logger as l

import datetime
import calendar

class Spark(object):
	"""Delete Spark old checkpoints files from Hadoop Cluster"""
	def __init__(self):
		super(Spark, self).__init__()
		self.namenode_ip = namenode_ip
		self.namenode_port = namenode_port
		self.file_user = file_user
		self.directories = directories
		self.deleteddirs = []

	def launcher(self):
		""" Send remove checkpoints task """

		# Connect
		client = InsecureClient('http://{ip}:{port}'.format(
								ip=self.namenode_ip, port=self.namenode_port),
								user=self.file_user)

		# Get current timestamp
		timenow = calendar.timegm(datetime.datetime.now().timetuple())
		unix_timestamp = int(timenow * 1000)
		onehour = 3600000
		todelete = int(unix_timestamp - onehour)

		# Return file name list
		for directory in self.directories:
			fnames = client.list(directory, status=True)

			# Fetch list and sets modificationTime
			for fname in fnames:
				ctime = fname[1]['modificationTime']
				if ctime <= todelete:
					dirtodelete = fname[1]['pathSuffix']
					client.delete('{directory}/{dirtodelete}'.format(directory=directory,
									dirtodelete=dirtodelete), recursive=True)
					l.info('Removing {dir} ...Removed!'.format(dir=dirtodelete))
					message = self.deleteddirs.append(dirtodelete)
				else:
					l.info('Nothing to remove into {directory}. Bye bye!'.format(
							directory=directory))

		if message:
			stdout = message
		else:
			stdout = 'No directories were deleted.'

		return {
				'Deleted directories': stdout
				}
