# -*- coding: utf-8 -*-
"""
Created on Mon May  4 11:18:48 2026

@author: denis
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dags'))

import io
from repositories.graph_job_repository import GraphJobRepository
from ftpConnector import ftpConnector

job = GraphJobRepository.get_job_for_execution()
if not job:
    print("No job available")
    sys.exit(0)

try:
    fileName = 'graphJobs/' + str(job[0]) + '/textCorpuses.txt'
    fileWithPaths = ftpConnector.getFile(fileName, 'Graph')
except Exception as e:
    print(e)
