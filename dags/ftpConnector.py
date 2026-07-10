# -*- coding: utf-8 -*-

from configs import getConfig
import ftplib
import io


def _ftp_mkdirs(server, path):
    parts = [p for p in path.split('/') if p]
    for i in range(len(parts)):
        partial = '/'.join(parts[:i + 1])
        try:
            server.mkd(partial)
        except ftplib.error_perm as e:
            if not str(e).startswith('550'):
                raise


FTP_TIMEOUT = 30


class ftpConnector:
    def storeFile(filename, file, ftpPostfix = ''):
        config = getConfig()
        server = ftplib.FTP(timeout=FTP_TIMEOUT)
        server.connect(config["FtpHost" + ftpPostfix], config["FtpPort" + ftpPostfix])
        server.login(config["FtpUser" + ftpPostfix],config["FtpPassword" + ftpPostfix])
        parent = '/'.join(filename.split('/')[:-1])
        if parent:
            _ftp_mkdirs(server, parent)
        server.storbinary(f"STOR {filename}", file)
        server.quit()
    def getFile(filePath, ftpPostfix = ''):
        config = getConfig()
        server = ftplib.FTP(timeout=FTP_TIMEOUT)
        server.connect(config["FtpHost" + ftpPostfix], config["FtpPort" + ftpPostfix])
        server.login(config["FtpUser" + ftpPostfix],config["FtpPassword" + ftpPostfix])
        memfile = io.BytesIO()
        server.retrbinary("RETR " + filePath, memfile.write)
        server.quit()
        return memfile
    def getFileList(path, ftpPostfix = ''):
        config = getConfig()
        server = ftplib.FTP(timeout=FTP_TIMEOUT)
        server.connect(config["FtpHost" + ftpPostfix], config["FtpPort" + ftpPostfix])
        server.login(config["FtpUser" + ftpPostfix],config["FtpPassword" + ftpPostfix])
        files = server.nlst(path)
        server.quit()
        return files
