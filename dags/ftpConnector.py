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


def _safe_close(server):
    # quit() sends a QUIT command and waits on the socket - if the
    # connection is already broken (e.g. the failure we're closing after
    # was itself a socket error) quit() can raise too. Fall back to a
    # local-only close() so the session is never left dangling either way.
    # See the 2026-07-16 incident: unclosed sessions from failed
    # storbinary/retrbinary/nlst calls accumulated server-side under
    # CONCURRENCY=64 until FileZilla silently stopped accepting new
    # passive data connections while still reporting "active" to systemd.
    try:
        server.quit()
    except Exception:
        try:
            server.close()
        except Exception:
            pass


class ftpConnector:
    def storeFile(filename, file, ftpPostfix = ''):
        config = getConfig()
        server = ftplib.FTP(timeout=FTP_TIMEOUT)
        try:
            server.connect(config["FtpHost" + ftpPostfix], config["FtpPort" + ftpPostfix])
            server.login(config["FtpUser" + ftpPostfix],config["FtpPassword" + ftpPostfix])
            parent = '/'.join(filename.split('/')[:-1])
            if parent:
                _ftp_mkdirs(server, parent)
            server.storbinary(f"STOR {filename}", file)
        finally:
            _safe_close(server)
    def getFile(filePath, ftpPostfix = ''):
        config = getConfig()
        server = ftplib.FTP(timeout=FTP_TIMEOUT)
        try:
            server.connect(config["FtpHost" + ftpPostfix], config["FtpPort" + ftpPostfix])
            server.login(config["FtpUser" + ftpPostfix],config["FtpPassword" + ftpPostfix])
            memfile = io.BytesIO()
            server.retrbinary("RETR " + filePath, memfile.write)
        finally:
            _safe_close(server)
        return memfile
    def getFileList(path, ftpPostfix = ''):
        config = getConfig()
        server = ftplib.FTP(timeout=FTP_TIMEOUT)
        try:
            server.connect(config["FtpHost" + ftpPostfix], config["FtpPort" + ftpPostfix])
            server.login(config["FtpUser" + ftpPostfix],config["FtpPassword" + ftpPostfix])
            files = server.nlst(path)
        finally:
            _safe_close(server)
        return files
