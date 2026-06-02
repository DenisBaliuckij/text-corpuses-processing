# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 11:08:12 2026

@author: denis
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dags'))

import io
from repositories.graph_job_repository import GraphJobRepository
from ftpConnector import ftpConnector

job = GraphJobRepository.get_job_for_preparation()
if not job:
    print("No jobs to process")
else:
    try:
        path = job[2]
        jobId = job[0]
        paths = path.split(';')
        fileList = []
        for pt in paths:
            fileList += ftpConnector.getFileList(pt, "Tex")
        """file = io.BytesIO(fileList)
        fileName = 'graphJobs/'+job[0]+'/textCorpuses.txt'
        ftpConnector.storeFile(filename, file, 'Graph')"""
        for file in fileList:
            GraphJobRepository.add_file_source(file, jobId)

    except Exception as e:
        print(e)
        GraphJobRepository.set_job_error(job[0], e)   

    