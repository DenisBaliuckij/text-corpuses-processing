# -*- coding: utf-8 -*-

from configs import getConfig
import ftplib

class ftpConnector:
    def storeFile(filename, file):
        config = getConfig()
        server = ftplib.FTP()
        server.connect(config["FtpHost"], config["FtpPort"])
        server.login(config["FtpUser"],config["FtpPassword"])
        server.storbinary(f"STOR {filename}", file)
        server.quit()