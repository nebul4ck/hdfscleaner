# -*- encoding: utf-8 -*-

"""
.. module:: main
   :platform: Unix/Linux
   :synopsis: Auto loader class.
.. moduleauthor::
   :Nickname: nebul4ck
   :mail:  a.gonzalezmesas@gmail.com
   :Web :
"""

from importlib import import_module

class AppLoader(object):
	""" Automatic class loader """
	def __init__(self, lib_path):
		super(AppLoader, self).__init__()
		self.lib_path = lib_path

	def get_instance(self, appName):
		""" Application name capitalize to import app class. Ej druid =>\
		Druid (class) """
		className = str(appName).capitalize()

		try:
			loadMod = import_module('{path}.{app}'.format(path=self.lib_path, app=className))
		except ImportError:
			import traceback
			traceback.print_exc()
			raise
		else:
			""" Class initiation """
			return getattr(loadMod, className)()
