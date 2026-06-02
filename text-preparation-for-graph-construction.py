# -*- coding: utf-8 -*-
"""
Created on Sun May 17 10:54:06 2026

@author: denis
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dags'))

import io
import json
from repositories.graph_job_repository import GraphJobRepository
from ftpConnector import ftpConnector
from anaphoraResolverLapinLiass import BatchAnaphoraResolver, resolve_and_substitute

"""get list of files"""
job = GraphJobRepository.get_job_for_preparation()
if job is None:
    print("No jobs to prepare")
else:
    jobId = job[0]
    jobConfig = json.loads(job[3])
    print(jobConfig["processorName"])
    if jobConfig["processorName"] == 'RuleBased':
        filePath = GraphJobRepository.get_text_source(jobId)
        fileToProcess = ftpConnector.getFile(filePath[1], 'Tex')
        fileToProcess.seek(0)
        text = str(fileToProcess.read())
        resolvedAnaphore = BatchAnaphoraResolver().resolve_document(text)   
        output, substitutions, resolutions = resolve_and_substitute(text)
        print(output)
"""add list to the table"""