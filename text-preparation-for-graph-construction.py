# -*- coding: utf-8 -*-
"""
Created on Sun May 17 10:54:06 2026

@author: denis
"""

import dbConnector
from dbConnector import databaseConnector
import ftpConnector
from ftpConnector import ftpConnector
import io
import anaphoraResolverLapinLiass
from anaphoraResolverLapinLiass import BatchAnaphoraResolver, resolve_and_substitute
import json

"""get list of files"""
job = databaseConnector.getJobForPreparation()
if job is None:
    print("No jobs to prepare")
else:
    jobId = job[0]
    jobConfig = json.loads(job[3])
    print(jobConfig["processorName"])
    if jobConfig["processorName"] == 'RuleBased':
        filePath = databaseConnector.getTextSourceForProcessing(jobId)
        fileToProcess = ftpConnector.getFile(filePath[1], 'Tex')
        fileToProcess.seek(0)
        text = str(fileToProcess.read())
        resolvedAnaphore = BatchAnaphoraResolver().resolve_document(text)   
        output, substitutions, resolutions = resolve_and_substitute(text)
        print(output)
"""add list to the table"""