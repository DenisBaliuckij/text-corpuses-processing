import time
import pyodbc
from configs import getConfig

_DEADLOCK_RETRIES = 3


def _exec_write(sql, params=()):
    # SQL Server error 40001 = deadlock victim; retry with back-off
    for attempt in range(_DEADLOCK_RETRIES):
        try:
            cnxn = pyodbc.connect(getConfig()['ConnectionString'])
            cursor = cnxn.cursor()
            cursor.execute(sql, params)
            cnxn.commit()
            cursor.close()
            cnxn.close()
            return
        except pyodbc.Error as e:
            if e.args[0] == '40001' and attempt < _DEADLOCK_RETRIES - 1:
                time.sleep(0.1 * (attempt + 1))
            else:
                raise


class ProxyRepository:
    @staticmethod
    def add_or_update(ip, port, last_checked, protocols):
        _exec_write(
            "execute [dbo].[AddOrUpdateProxy] @ip = ?, @port = ?, @lastChecked = ?, @protocols = ?",
            (ip, port, last_checked, protocols)
        )

    @staticmethod
    def mark_broken(ip):
        _exec_write("execute [dbo].[MarkProxyAsBroken] @ip = ?", (ip,))

    @staticmethod
    def get_latest() -> dict:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetLatestProxy]")
        row = cursor.fetchone()
        cursor.close()
        cnxn.close()
        return {'proxieIp': row[0], 'proxiePort': row[1], 'proxieProtocol': row[2]}
