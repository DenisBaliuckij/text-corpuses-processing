import pyodbc
from configs import getConfig


class ServiceStateRepository:
    @staticmethod
    def get(service_id):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetServiceState] @serviceId = ?", (service_id,))
        row = cursor.fetchone()
        cursor.close()
        cnxn.close()
        return row

    @staticmethod
    def update(service_id, state):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[UpdateState] @serviceID = ?, @state = ?", (service_id, state)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def remove(service_id):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[RemoveServiceState] @serviceID = ?", (service_id,))
        cnxn.commit()
        cursor.close()
        cnxn.close()
