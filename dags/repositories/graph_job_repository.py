import pyodbc
from configs import getConfig


class GraphJobRepository:
    @staticmethod
    def insert_job(config, paths):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[AddGraphCreationJob] @config = ?, @paths=?", (config, paths)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def get_job_for_preparation():
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetJobForPreparation]")
        row = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return row

    @staticmethod
    def set_job_error(job_id, error):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[SetErrorForGraphCreationJob] @id = ?, @error = ?",
            (job_id, str(error))
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def transition_to_execution(job_id):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[TransitionJobToExecution] @jobId = ?", (job_id,))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def process_to_text_copying(job_id):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[ProcessGraphCreationJobToTextCopying] @jobId = ?", (job_id,)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def get_processor_config(job_id) -> str | None:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "SELECT ProcessorConfig FROM [dbo].[GraphConstructionJob] WHERE ID = ?", (job_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        cnxn.close()
        return row[0] if row else None

    @staticmethod
    def get_files_for_job(job_id) -> list:
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "SELECT ID FROM [dbo].[GraphConstructionFiles] WHERE GraphConstructionJobId = ? AND Status = 20",
            (job_id,)
        )
        rows = cursor.fetchall()
        cursor.close()
        cnxn.close()
        return rows

    @staticmethod
    def add_file_source(location, job_id):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[AddTextSourceForProcessing] @location = ?, @jobId = ?",
            (location, job_id)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def get_file_for_anaphora():
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetFileForAnaphoraResolution]")
        row = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return row

    @staticmethod
    def mark_anaphora_done(file_id, resolved_path):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[MarkFileAnaphoraDone] @fileId = ?, @resolvedFilePath = ?",
            (file_id, resolved_path)
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def get_file_for_graph_building():
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetFileForGraphBuilding]")
        row = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return row

    @staticmethod
    def mark_graph_done(file_id):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[MarkFileGraphDone] @fileId = ?", (file_id,))
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def set_file_error(file_id, error):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute(
            "execute [dbo].[SetFileError] @fileId = ?, @error = ?", (file_id, str(error))
        )
        cnxn.commit()
        cursor.close()
        cnxn.close()

    @staticmethod
    def finalize_completed_jobs():
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[FinalizeCompletedJobs]")
        row = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return row

    @staticmethod
    def get_job_for_execution():
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetJobForExecution]")
        row = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return row

    @staticmethod
    def get_text_source(job_id):
        cnxn = pyodbc.connect(getConfig()['ConnectionString'])
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetTextSourceForProcessing] @jobId=?", (job_id,))
        row = cursor.fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()
        return row
