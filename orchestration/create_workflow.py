# =============================================================================
# FASE 4: Orquestación del Pipeline — Databricks Workflow
# =============================================================================
# Este script genera la definición JSON del Databricks Workflow
# que orquesta el pipeline completo Bronze → Silver → Gold → Quality.
#
# También se puede ejecutar directamente como API call via Databricks CLI:
#   databricks jobs create --json @workflow_definition.json
#
# Alternativa: Azure Data Factory invoca los notebooks via Linked Service
# =============================================================================

import json

WORKFLOW_DEFINITION = {
    "name": "finbank_medallion_pipeline",
    "description": "Pipeline end-to-end FinBank: Bronze → Silver → Gold → Quality Checks",

    # --- Programación: Diario a las 02:00 COT (07:00 UTC) ---
    "schedule": {
        "quartz_cron_expression": "0 0 7 * * ?",  # 02:00 COT = 07:00 UTC
        "timezone_id": "America/Bogota",
        "pause_status": "UNPAUSED"
    },

    # --- Cluster compartido para todas las tareas ---
    "job_clusters": [
        {
            "job_cluster_key": "etl_cluster",
            "new_cluster": {
                "spark_version": "14.3.x-scala2.12",
                "node_type_id": "Standard_DS3_v2",
                "autoscale": {
                    "min_workers": 1,
                    "max_workers": 4
                },
                "spark_conf": {
                    "spark.databricks.delta.optimizeWrite.enabled": "true",
                    "spark.databricks.delta.autoCompact.enabled": "true",
                    "spark.sql.adaptive.enabled": "true"
                },
                "spark_env_vars": {
                    "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                },
                "data_security_mode": "SINGLE_USER",
                "runtime_engine": "STANDARD"
            }
        }
    ],

    # --- Tareas del DAG con dependencias explícitas ---
    "tasks": [
        # ---- TASK 1: Bronze Ingestion ----
        {
            "task_key": "bronze_ingestion",
            "description": "Ingesta de datos crudos desde SQL Server a ADLS Gen2 Bronze",
            "job_cluster_key": "etl_cluster",
            "notebook_task": {
                "notebook_path": "/Repos/finbank-pipeline/pipelines/bronze/ingest_bronze",
                "base_parameters": {
                    "storage_account": "{{job.parameters.storage_account}}",
                    "sql_server": "{{job.parameters.sql_server}}",
                    "sql_database": "{{job.parameters.sql_database}}",
                    "execution_mode": "incremental"
                }
            },
            "timeout_seconds": 1800,  # 30 min
            "retry_on_timeout": True,
            "max_retries": 3,
            "min_retry_interval_millis": 60000,  # 1 min (exponential backoff)
        },

        # ---- TASK 2: Silver Transformation ----
        {
            "task_key": "silver_transformation",
            "description": "Limpieza, validación, enmascaramiento PII y flag sospechoso",
            "depends_on": [{"task_key": "bronze_ingestion"}],
            "job_cluster_key": "etl_cluster",
            "notebook_task": {
                "notebook_path": "/Repos/finbank-pipeline/pipelines/silver/transform_silver",
                "base_parameters": {
                    "storage_account": "{{job.parameters.storage_account}}",
                    "batch_id": "{{tasks.bronze_ingestion.values.bronze_batch_id}}"
                }
            },
            "timeout_seconds": 2700,  # 45 min
            "retry_on_timeout": True,
            "max_retries": 3,
            "min_retry_interval_millis": 120000,
        },

        # ---- TASK 3: Gold Business Logic ----
        {
            "task_key": "gold_transformation",
            "description": "Modelo dimensional, reglas de negocio, KPIs ejecutivos",
            "depends_on": [{"task_key": "silver_transformation"}],
            "job_cluster_key": "etl_cluster",
            "notebook_task": {
                "notebook_path": "/Repos/finbank-pipeline/pipelines/gold/transform_gold",
                "base_parameters": {
                    "storage_account": "{{job.parameters.storage_account}}",
                    "batch_id": "{{tasks.bronze_ingestion.values.bronze_batch_id}}"
                }
            },
            "timeout_seconds": 3600,  # 60 min
            "retry_on_timeout": True,
            "max_retries": 3,
            "min_retry_interval_millis": 120000,
        },

        # ---- TASK 4: Quality Checks ----
        {
            "task_key": "quality_checks",
            "description": "5 verificaciones automatizadas sobre capa Gold",
            "depends_on": [{"task_key": "gold_transformation"}],
            "job_cluster_key": "etl_cluster",
            "notebook_task": {
                "notebook_path": "/Repos/finbank-pipeline/pipelines/quality/quality_checks",
                "base_parameters": {
                    "storage_account": "{{job.parameters.storage_account}}",
                    "batch_id": "{{tasks.bronze_ingestion.values.bronze_batch_id}}"
                }
            },
            "timeout_seconds": 900,  # 15 min
            "max_retries": 1,
        },

        # ---- TASK 5: Notification (Success) ----
        {
            "task_key": "send_success_notification",
            "description": "Enviar resumen diario de ejecución exitosa",
            "depends_on": [{"task_key": "quality_checks"}],
            "job_cluster_key": "etl_cluster",
            "notebook_task": {
                "notebook_path": "/Repos/finbank-pipeline/orchestration/send_notification",
                "base_parameters": {
                    "status": "SUCCESS",
                    "storage_account": "{{job.parameters.storage_account}}",
                    "batch_id": "{{tasks.bronze_ingestion.values.bronze_batch_id}}"
                }
            },
            "timeout_seconds": 300,
        },
    ],

    # --- Parámetros del Job ---
    "parameters": [
        {"name": "storage_account", "default": "stfinbankdatalakedev"},
        {"name": "sql_server", "default": "sql-finbank-source-dev.database.windows.net"},
        {"name": "sql_database", "default": "finbank_transactional"},
    ],

    # --- Alertas por email ante fallo ---
    "email_notifications": {
        "on_failure": ["data-engineering@finbank.com"],
        "on_success": ["data-engineering@finbank.com"],
        "no_alert_for_skipped_runs": True
    },

    "webhook_notifications": {},

    # --- Tags ---
    "tags": {
        "project": "finbank-pipeline",
        "layer": "orchestration",
        "environment": "dev"
    },

    "max_concurrent_runs": 1,
    "format": "MULTI_TASK"
}

# Guardar definición
output_path = "workflow_definition.json"
with open(output_path, "w") as f:
    json.dump(WORKFLOW_DEFINITION, f, indent=2)

print(f"Workflow definition saved to {output_path}")
print(f"\nDeploy with: databricks jobs create --json @{output_path}")
