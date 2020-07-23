# -*- encoding: utf-8 -*-

"""
.. module:: main
   :platform: GNU/Linux
   :synopsis: Clean Hadoop old files.
.. moduleauthor::
   :Nickname: Alberto González
   :mail: a.gonzalezmesas@gmail.com
   :Web :	https://github.com/nebul4ck/hdfscleaner
"""

import requests
import time
import re
import json
import psycopg2
#import MySQLdb

from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from random import randint
from operator import itemgetter

from hdfscleaner.conf.druidConf import *
from hdfscleaner.lib.logger import logger as l

class Druid(object):
	"""Delete Druid old files from Hadoop Cluster"""
	def __init__(self):
		super(Druid, self).__init__()
		self.url_coordinator = url_coordinator
		self.url_overlord = url_overlord
		self.find_datasource = find_datasource
		self.find_rules_datasource = find_rules_datasource
		self.launch_task = launch_task
		self.headers = headers
		self.loadByPeriod = loadByPeriod
		self.key_period = key_period
		self.key_type = key_type

	def calc_dates(self):
		""" This function returns the date for today. With this date the second interval from segment
		timestamp will be calculated """
		str_datetime_today = str(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
		str_datetime_tuple = time.strptime(str_datetime_today, "%Y-%m-%d %H:%M:%S")
		year =  str_datetime_tuple[0]
		month = str_datetime_tuple[1]
		day = str_datetime_tuple[2]
		hour = str_datetime_tuple[3]
		date_today = datetime(year,month,day).date()

		return date_today, hour

	def ds_list(self):
		""" Returns a enabled datasources list """
		get_datasources = requests.get('{url}{find_datasource}'.format(
			url=self.url_coordinator, find_datasource=self.find_datasource))
		l_datasources = get_datasources.json()

		return l_datasources

	def check_rules(self, l_datasources):
		""" Checks rules for each datasource in l_datasources and get older segment """
		datasource_metadatas = []

		for datasource in l_datasources:
			datasource = str(datasource)

			try:
				ds_rules = requests.get('{url}{find_rules_datasource}/{datasource}?full'.format(url=self.url_coordinator,
					find_rules_datasource=self.find_rules_datasource,
					datasource=datasource))
				# IMPORTANTE: La siguiente variable no devuelve los segmentos con Used = 0. La API de Druid no va bien
				# En realidad con esta consulta en mysql:
				# select id from druid_segments where used = 0;
				# Podemos obtener todos los segmentos con valor de uso 0 y eliminarlos:
				# spark-ef-suricata_2017-10-31T03:00:00.000Z_2017-10-31T04:00:00.000Z_2017-10-31T03:00:17.984Z
				# select id from druid_segments where used = 0 and id like "%icmp%";
				# IMPLEMENTAR LA LISTA DE MYSQL DEVUELTA.

				#ds_segments = requests.get('{url}{find_datasource}/{datasource}/segments'.format(url=self.url_coordinator,
				#												find_datasource=self.find_datasource,
				#												datasource=datasource))
			except KeyError as e:
				raise

			rules = ds_rules.json()
			#older_segment = ds_segments.json()[0].split('_')[1]

			metadata = {
						datasource: {
										'rules': rules
										#'older_segment': older_segment
									}
						}

			datasource_metadatas.append(metadata)

		return datasource_metadatas

	def set_interval(self, datasource):

		''' TODO: delete invertval from postgresql also, not only from hdfs'''
		stderr = ''

		sql_conn = None

		# db = MySQLdb.connect(host=coordinator_host,
		# 	user=user_db,
		# 	passwd=pass_user_db,
		# 	db=db_druid)

		try:

			sql_conn = psycopg2.connect(host=postgresql_host, database=db_druid,
										user=user_db, password=pass_user_db)

			cursor = sql_conn.cursor()

			#query = 'select id from {table} where used = 0 and id like "%{datasource}%";'.format(
			#	table=table_segments, datasource=datasource)

			query = "SELECT id FROM {table} WHERE used='f' AND id LIKE '%{source}%' ORDER BY start ASC;".format(
				table=table_segments, source=datasource)

			num_lines = cursor.execute(query)
			list_rows = cursor.fetchall()

			first_and_end = itemgetter(0, -1)(list_rows)
			older_segment = itemgetter(0)(first_and_end)[0].split('_')[1]
			newest_segment = itemgetter(-1)(first_and_end)[0].split('_')[1]

			if older_segment == newest_segment:
				hour = timedelta(hours=01)
				time_format = "%H:%M:%S"
				str_hour_old_segment = older_segment.split('T')[1].split('.')[0]
				hour_older_segment = datetime.strptime(str_hour_old_segment, time_format)
				newest_segment_hour = (hour_older_segment + hour).strftime(time_format)
				str_date_old_segment = older_segment.split('T')[0]
				newest_segment = '{str_date_old_segment}T{newest_segment_hour}.000Z'.format(
					str_date_old_segment=str_date_old_segment,
					newest_segment_hour=newest_segment_hour)

			interval = '{older_segment}/{newest_segment}'.format(older_segment=older_segment,
				newest_segment=newest_segment)

			sql_conn.close()

		except Exception:
			stderr = 'Datasource {datasource} hasn\'t segments for drop'.format(
				datasource=datasource)

		except (Exception, psycopg2.DatabaseError) as error:
			stderr = error

		finally:
			if sql_conn is not None:
				sql_conn.close()
				print('Database connection closed.')

		if not stderr:
			msg_return = {
							'interval': 'y',
							'response': interval
						}
		else:
			msg_return = {
							'interval': 'n',
							'response': stderr
						}

		return msg_return

	def set_killtasks(self, datasource_metadatas, hour, date_today):
		""" Finds the rule loadByPeriod, gets the period (ej P1M, 1 month) and launches kill segments task """
		kill_tasks = []
		msg_stderr = []

		# [{datasource: {'rules': [{u'type': u'loadByPeriod', u'period': u'P2M', u'tieredReplicants': {u'_default_tier': 1}},
		#{u'type': u'dropForever'}, {u'type': u'loadByPeriod', u'period': u'P1M', u'tieredReplicants': {u'_default_tier': 1}}],
		#'older_segment': u'2017-10-31T17:00:00.000Z'}}]
		for item in datasource_metadatas:
			datasources = item.keys()

			for datasource in datasources:
				# La primera regla en la lista es la regla activa en Druid. Si es loadbyperiod puede ser la definida por nosotros
				# o la _default, si no es loadbyperiod debe de dar error porque no es el comportamiento esperado de Druid.
				rule = item[datasource]['rules'][0]['type']

				# Create interval
				interval = self.set_interval(datasource)

				if rule == loadByPeriod:

					""" El siguiente bloque tenía en cuenta la fecha actual y restaba P2M (o lo definido por la regla).
					A partir de ahí se hacian las operaciones para calcular cual sería el segmento último a eliminar.
					Al no devolver la API del coordinator los segmentos con Used = 0, he tenido que emplear MySQL y cojo
					directamente los segmentos a 0 marcados por Druid, serán esos los que haya que eliminar.

			 		period = item[datasource]['rules'][0]['period']
			 		period_months = int(re.search(r'\d+', period).group())
					months_rule = relativedelta(months=period_months)

					date_subtraction = str(date_today - months_rule)

					# The last segment to drop
					# IMPORTANTE este lo podemos coger también de mysql
					newest_segment = '{date_subtraction}T{hour}:00:00.000Z'.format(date_subtraction=date_subtraction,
																					hour=hour)

					# EL INTERVALO ME LO ESTÁ DANDO MAL, DEPURAR. EL OLDER SEGMENT VIENE MAL
					complete_interval = '{older_segment}/{newest_segment}'.format(older_segment=older_segment,
																			newest_segment=newest_segment)
					"""

					if interval['interval'] == 'y':
						# kill_segment.json
						random = randint(0,1000)
						ts = int(time.time())
						ids = random + ts
						kill_segment = {
						 				'type':'kill',
										'id': ids,
										'dataSource': datasource,
										'interval': interval['response']
										}

						kill_segment_json = json.dumps(kill_segment)
						kill_tasks.append(kill_segment_json)
					else:
						stderr = interval['response']
						msg_stderr.append(stderr)
				else:
					stderr = 'Datasource {datasource} hasn\'t defined a loadByPeriod rule, nothing to do for {datasource}'.format(
						datasource=datasource)
					msg_stderr.append(stderr)

		return kill_tasks, msg_stderr

	def send_tasks(self, kill_tasks):
		status_tasks = []
		msg_stderr = []

		try:
			for task in kill_tasks:
				task_json = json.loads(task)
				datasource = task_json['dataSource']
				s_task = requests.post('{url}{launch_task}'.format(url=url_overlord, launch_task=launch_task),
					json=task_json)
				status_code = s_task.status_code
				response = s_task.text

				msg_stdout = {
								'Datasource': datasource,
								'Status_code': status_code,
								'Response': response
							}

				status_tasks.append(msg_stdout)
		except Exception as error:
			msg_stderr = msg_stderr.append(error)

		return status_tasks, msg_stderr

	def launcher(self):
		""" Send kill_segment_json task """
		date_today, hour = self.calc_dates()

		l_datasources = self.ds_list()

		datasource_metadatas = self.check_rules(l_datasources)

		tasks, stderr = self.set_killtasks(datasource_metadatas, hour, date_today)

		if tasks is not None:
			status_tasks, msg_stderr = self.send_tasks(tasks)
			if msg_stderr:
				stderr = [stderr, msg_stderr]
		else:
			status_tasks = 'Not tasks found to launch. Nothing to do, Bye bye...'

		return {
				'Http_response': status_tasks,
				'std_tasks': stderr
				}
