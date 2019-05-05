# -*- coding: utf-8 -*-

import logging

logFile = '/var/log/hdfscleaner/hdfscleaner.log'

logging.basicConfig(level=logging.INFO,
                                        format='%(asctime)s %(levelname)s %(filename)s(%(lineno)d): %(message)4s',
                                        datefmt='[%d/%m/%Y - %H:%M]',
                                        filename='{log}'.format(log=logFile),
                                        filemode='a')

logger = logging
