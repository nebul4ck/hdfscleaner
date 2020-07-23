# -*- encoding: utf-8 -*-

""" Druid Settings """

# DRUID-COORDINATOR
coordinator_host = 'druidcoordinator01'
coordinator_port = '8084'
url_coordinator = 'http://{coordinator_host}:{coordinator_port}'.format(
	coordinator_host=coordinator_host,
	coordinator_port=coordinator_port)

# DRUID-OVERLORD
overlord_host = 'druidoverlord01'
overlord_port = '8090'
url_overlord = 'http://{overlord_host}:{overlord_port}'.format(
	overlord_host=overlord_host,
	overlord_port=overlord_port)

# REQUESTS
find_datasource = '/druid/coordinator/v1/metadata/datasources'
find_rules_datasource = '/druid/coordinator/v1/rules'
launch_task = '/druid/indexer/v1/task'
headers = {'Content-Type': 'application/json'}

# RULES SETTINGS
loadByPeriod = 'loadByPeriod'
key_period = 'period'
key_type = 'type'

# PostgreSQL SETTINGS
postgresql_host = 'postgresql01'
user_db = 'druid'
pass_user_db = 'pasword'
db_druid = 'druid'
table_segments = 'druid_segments'
