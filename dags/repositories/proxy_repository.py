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
    def mark_success(ip):
        """Records that a proxy actually completed a real transfer, so
        GetLatestProxy/GetLatestFreeProxy can prefer proxies with a proven
        track record over freshly-imported, untested ones."""
        _exec_write("execute [dbo].[MarkProxySuccess] @ip = ?", (ip,))

    @staticmethod
    def get_latest() -> dict:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetLatestProxy]")
        row = cursor.fetchone()
        cursor.close()
        cnxn.close()
        return {'proxieIp': row[0], 'proxiePort': row[1], 'proxieProtocol': row[2]}

    @staticmethod
    def get_latest_free() -> dict:
        """Same as get_latest() but excludes the shared paid (BrightData)
        proxy, for callers that need a free-pool proxy specifically
        (e.g. file downloads, where BrightData has proven unreliable)."""
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetLatestFreeProxy]")
        row = cursor.fetchone()
        cursor.close()
        cnxn.close()
        return {'proxieIp': row[0], 'proxiePort': row[1], 'proxieProtocol': row[2]}

    @staticmethod
    def get_top_candidates(top_n=50) -> list:
        """Returns the top-N proxies by the same ranking GetLatestProxy uses
        (SuccessCount desc, LastChecked desc) - i.e. the proxies actually
        being selected for real downloads, for the validate_proxies DAG to
        re-test on a schedule."""
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetTopProxiesForValidation] @topN = ?", (top_n,))
        rows = cursor.fetchall()
        cursor.close()
        cnxn.close()
        return [(row[0], row[1], row[2]) for row in rows]
