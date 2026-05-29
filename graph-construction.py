# -*- coding: utf-8 -*-
"""
Created on Mon May  4 11:18:48 2026

@author: denis
"""

import sys
import dbConnector
from dbConnector import databaseConnector
import ftpConnector
from ftpConnector import ftpConnector
import io

job = databaseConnector.getJobForExecution()
if not job:
    print("No job available")
    sys.exit(0)

try:
    fileName = 'graphJobs/' + str(job[0]) + '/textCorpuses.txt'
    fileWithPaths = ftpConnector.getFile(fileName, 'Graph')
except Exception as e:
    print(e)
