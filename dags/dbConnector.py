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
        cursor.execute("execute [dbo].[RemoveServiceState] @serviceID = ?", (str(proxieIp).strip()))
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
        result = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return result

    def addFileSourceForGraphConstructionJob(location, jobId):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[AddTextSourceForProcessing] @location = ?, @jobId = ?", (location, jobId))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    def processGraphCreationJobToTextCopying(jobId):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[ProcessGraphCreationJobToTextCopying] @jobId = ?", (jobId,))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    def setErrorForPreparationJob(jobId, error):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[SetErrorForGraphCreationJob] @id = ?, @error = ?", (jobId, str(error)))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    def transitionJobToExecution(jobId):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[TransitionJobToExecution] @jobId = ?", (jobId,))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    def getFileForAnaphoraResolution():
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetFileForAnaphoraResolution]")
        result = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return result

    def markFileAnaphoraDone(fileId, resolvedFilePath):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[MarkFileAnaphoraDone] @fileId = ?, @resolvedFilePath = ?", (fileId, resolvedFilePath))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    def setFileError(fileId, error):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[SetFileError] @fileId = ?, @error = ?", (fileId, str(error)))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    def getFileForGraphBuilding():
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetFileForGraphBuilding]")
        result = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return result

    def markFileGraphDone(fileId):
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[MarkFileGraphDone] @fileId = ?", (fileId,))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    def finalizeCompletedJobs():
        cnxn = pyodbc.connect(getConfig()["ConnectionString"])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[FinalizeCompletedJobs]")
        result = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return result

