import pyodbc
from configs import getConfig


class ProxyRepository:
    @staticmethod
    def add_or_update(ip, port, last_checked, protocols):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[AddOrUpdateProxy] @ip = ?, @port = ?, @lastChecked = ?, @protocols = ?",
            (ip, port, last_checked, protocols)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def mark_broken(ip):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[MarkProxyAsBroken] @ip = ?", (ip,))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def get_latest() -> dict:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetLatestProxy]")
        row = cursor.fetchone()
        cursor.close()
        cnxn.close()
        return {'proxieIp': row[0], 'proxiePort': row[1], 'proxieProtocol': row[2]}
