# -*- coding: utf-8 -*-
"""
Created on Wed Apr 29 16:04:14 2026

@author: denis
"""

import pendulum

from airflow.sdk import DAG, Param, get_current_context
from airflow.sdk import task


with DAG(
    dag_id="start_tree_formation_job",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs = 1,
    tags=["treeFormation"],
    params={
       "paths": Param("", type="string", title="Paths to use for text gathering"),
       "textProcessorName": Param("RuleBased",
            enum=["RuleBased", "AIBased"],
            description="Name of text processor.",
            title="Text processor name",
        ),
       "anaphoraResolverName": Param("LapinLiass",
            enum=["LapinLiass", "SpacyNeural"],
            description="Anaphora resolver to use.",
            title="Anaphora resolver",
        ),
   },
) as dag:
    @task()
    def insertGraphProcessingJob():

        import json
        from repositories.graph_job_repository import GraphJobRepository
        ctx = get_current_context()
        params = ctx["dag"].params
        config = {
            "processorName": params["textProcessorName"],
            "anaphoraResolverName": params["anaphoraResolverName"],
        }

        GraphJobRepository.insert_job(json.dumps(config), params["paths"])
        
    insertGraphProcessingJob()