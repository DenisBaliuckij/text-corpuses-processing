# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 11:08:12 2026

@author: denis
"""

import dbConnector
from dbConnector import databaseConnector
import ftpConnector
from ftpConnector import ftpConnector
import io

job = databaseConnector.getJobForPreparation()
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
            databaseConnector.addFileSourceForGraphConstructionJob(file, jobId)            
        
    except Exception as e:
        print(e)
        databaseConnector.setErrorForPreparationJob(job[0], e)   

    