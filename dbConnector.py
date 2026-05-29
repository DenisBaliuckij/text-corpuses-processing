# -*- coding: utf-8 -*-
"""
Created on Sun Mar 29 20:31:25 2026

@author: denis
"""
import pyodbc 
import configs
from configs import getConfig

class databaseConnector:
    def addOrUpdateProxy(ip, port, lastChecked, protocols):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[AddOrUpdateProxy] @ip = ?, @port = ?, @lastChecked = ?, @protocols = ?", (ip, port, lastChecked, protocols))
        cursor.close()
        cnxn.commit()
        cnxn.close()
    def markProxyAsBroken(ip):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[MarkProxyAsBroken] @ip = ?", (ip))
        cnxn.commit()
        cursor.close()
        cnxn.close()
    def getLatestProxy():
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetLatestProxy]")
        fetchResults = cursor.fetchone()
        proxieIp = fetchResults[0]
        proxiePort = fetchResults[1]
        proxieProtocol = fetchResults[2]
        cursor.close()
        cnxn.close()
        return {
                "proxieIp":proxieIp, 
                "proxiePort":proxiePort, 
                "proxieProtocol":proxieProtocol
            }
    def addPdfUrl(url):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[AddPdfUrl] @url = ?", (url))
        cursor.close()
        cnxn.commit()
        cnxn.close()
    def getServiceState(serviceId):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetServiceState] @serviceId = ?", (serviceId))
        result = cursor.fetchone()
        cursor.close()
        cnxn.close()
        return result
    def removeServiceState(serviceId):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[RemoveServiceState] @serviceID = ?", (serviceId,))
        cnxn.commit()
        cursor.close()
        cnxn.close()
    def updateServiceState(serviceID, state):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[UpdateState] @serviceID = ?, @state = ?", (serviceID, state))
        cnxn.commit()
        cursor.close()
        cnxn.close()
    def getPdfToDownload():
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetPdfToDownload]")
        fetchResults = cursor.fetchone()
        url = fetchResults[0]
        cursor.close()
        cnxn.close()
        return url
    def savePdfFileLocation(url, location):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[SavePdfFileLocation] @pdfUrl = ?, @fileLocation=?", (url, location))
        cnxn.commit()
        cursor.close()
        cnxn.close()
    def getPdfToConvertToLatex():
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetPDFLocationForLatexConvertation]")
        fetchResults = cursor.fetchone()
        url = fetchResults[0]
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return url
    def saveLatexFileLocation(url, location):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[SaveLatexDocumentLocation] @pdfUrl = ?, @latexLocation=?", (url, location))
        cnxn.commit()
        cursor.close()
        cnxn.close()
    def insertGraphCreationJob(config, paths):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[AddGraphCreationJob] @config = ?, @paths=?", (config, paths))
        cnxn.commit()
        cursor.close()
        cnxn.close()
    def getJobForPreparation():
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetJobForPreparation]")
        fetchResults = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return fetchResults
    def setErrorForPreparationJob(jobId, error):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[SetErrorForGraphCreationJob]  @id = ?, @error=?", (jobId, error))
        fetchResults = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
    def getJobForExecution():
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetJobForExecution]")
        fetchResults = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return fetchResults
    def addFileSourceForGraphConstructionJob(fileLocation, jobId):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[AddTextSourceForProcessing]  @location = ?, @jobId=?", (fileLocation, jobId))
        cnxn.commit()
        cursor.close()
        cnxn.close()
    def getTextSourceForProcessing(jobId):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetTextSourceForProcessing]   @jobId=?", (jobId))
        fetchResults = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return fetchResults

