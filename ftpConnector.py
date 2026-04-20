# -*- coding: utf-8 -*-

from configs import getConfig
import ftplib
import io

class ftpConnector:
    def storeFile(filename, file, ftpPostfix = ''):
        config = getConfig()
        server = ftplib.FTP()
        server.connect(config["FtpHost" + ftpPostfix], config["FtpPort" + ftpPostfix])
        server.login(config["FtpUser" + ftpPostfix],config["FtpPassword" + ftpPostfix])
        server.storbinary(f"STOR {filename}", file)
        server.quit()
    def getFile(filePath, ftpPostfix = ''):
        config = getConfig()
        server = ftplib.FTP()
        server.connect(config["FtpHost" + ftpPostfix], config["FtpPort" + ftpPostfix])
        server.login(config["FtpUser" + ftpPostfix],config["FtpPassword" + ftpPostfix])
        memfile = io.BytesIO()
        server.retrbinary("RETR " + filePath ,memfile.write)
        server.quit()
        return memfile