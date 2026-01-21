import json
from pathlib import Path
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


CONFIG_PATH = Path(__file__).parent / "tariff_dags_config.json"
with open(CONFIG_PATH, "r") as f:
    DAG_CONFIGS = json.load(f)

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(seconds=300),
}

def create_tariff_dag(cfg: dict) -> DAG:
    """
        Create a DAG for tariff data ingestion and processing, based on the provided configuration.
    """

    # Build Lambda event (Airflow → Lambda contract)
    def build_lambda_event():
        return {
            "source": cfg["source"],
            "domain": cfg["domain"],
            "dataset": cfg["dataset"],
            "api_params": cfg.get("api_params", {}),
            "triggered_at": datetime.now(timezone.utc).isoformat(),
        }
    
    dag = DAG(
        dag_id=cfg["dag_id"],
        schedule=cfg["schedule"],  # cron string
        start_date=datetime(2025, 7, 30, tzinfo=timezone.utc),
        catchup=False,
        default_args=DEFAULT_ARGS,
        tags=cfg.get("tags", [])
    )

    with dag:
        @task(task_id="start")
        def start():
            print(f"Starting ingestion for {cfg['source']}.{cfg['dataset']}")

        invoke_lambda = LambdaInvokeFunctionOperator(
            task_id="lambda_ingest_raw",
            function_name=cfg["lambda_function_name"],
            invocation_type="RequestResponse",
            payload=json.dumps(build_lambda_event()),
            aws_conn_id="aws_default",
            log_type="Tail",
            do_xcom_push=True,  # critical to push response to XCom for downstream tasks
        )

        @task(task_id="extract_lambda_result")
        def extract_lambda_result(lambda_response: dict, **context) -> dict:
            """
            Normalize and validate Lambda response.
            This is the single source of truth for downstream Glue jobs.
            """
            print("Extracting and validating Lambda response...")
            print(f"Lambda response: {lambda_response}")

            if isinstance(lambda_response, str):
                try:
                    lambda_response = json.loads(lambda_response)
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"Failed to decode Lambda response as JSON: {lambda_response}"
                    ) from e
                
            if lambda_response.get("status", None) != "SUCCESS":
                raise RuntimeError(f"Lambda ingestion failed: {lambda_response}")
            
            if lambda_response.get("record_count", 0) == 0:
                raise ValueError("No records ingested by Lambda.")

            return {
                "domain": lambda_response["domain"],
                "source": lambda_response["source"],
                "dataset": lambda_response["dataset"],
                "keys": lambda_response["keys"],
                "record_count": lambda_response["record_count"],
                "ingested_at": lambda_response["ingested_at"],
                "lambda_exec_ts": lambda_response["lambda_exec_ts"],
                "dag_id": cfg["dag_id"],
                "run_id": context["run_id"],
            }

        glue_job = GlueJobOperator(
            task_id="glue_job",
            job_name=cfg["glue_job"],
            aws_conn_id="aws_default",
            wait_for_completion=True,
            script_args={
                "--DOMAIN": "{{ ti.xcom_pull(task_ids='extract_lambda_result')['domain'] }}",
                "--SOURCE": "{{ ti.xcom_pull(task_ids='extract_lambda_result')['source'] }}",
                "--DATASET": "{{ ti.xcom_pull(task_ids='extract_lambda_result')['dataset'] }}",
                "--KEYS": "{{ ti.xcom_pull(task_ids='extract_lambda_result')['keys'] | tojson }}",
                "--RECORD_COUNT": "{{ ti.xcom_pull(task_ids='extract_lambda_result')['record_count'] }}",
                "--INGESTED_AT": "{{ ti.xcom_pull(task_ids='extract_lambda_result')['ingested_at'] }}",
                "--DAG_ID": "{{ ti.xcom_pull(task_ids='extract_lambda_result')['dag_id'] }}",
                "--RUN_ID": "{{ ti.xcom_pull(task_ids='extract_lambda_result')['run_id'] }}",
            },
        )

        @task(task_id="end")
        def end():
            print(f"Ingestion completed for {cfg['source']}.{cfg['dataset']}")

        start_task = start()
        lambda_result = extract_lambda_result(invoke_lambda.output)
        end_task = end()

        start_task >> invoke_lambda >> lambda_result
        lambda_result >> glue_job >> end_task

    return dag

for _, cfg in DAG_CONFIGS.items():
    globals()[cfg["dag_id"]] = create_tariff_dag(cfg)