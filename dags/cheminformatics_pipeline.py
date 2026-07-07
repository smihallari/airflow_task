import io
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from lib.utils.aws import get_s3_client
from soda.scan import Scan


def notify_teams(context):
    webhook_url = os.getenv("AIRFLOW_CONN_MSTEAMS_WEBHOOK")
    if not webhook_url:
        return

    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "Unknown Task"
    exception = context.get("exception", "No exception details provided.")

    adaptive_card = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.2",
        "body": [
            {
                "type": "TextBlock",
                "text": f"Pipeline Failure: {task_id}",
                "style": "heading",
                "size": "Large",
                "weight": "bolder",
                "wrap": True,
                "color": "attention",
            },
            {
                "type": "TextBlock",
                "text": f"Exception:\n\n{str(exception)}",
                "weight": "default",
                "wrap": True,
            },
        ],
    }

    payload = {
        "type": "message",
        "attachments": [
            {"contentType": "application/vnd.microsoft.card.adaptive", "content": adaptive_card}
        ],
    }

    try:
        requests.post(webhook_url, json=payload, headers={"Content-Type": "application/json"})
    except Exception as e:
        print(f"Failed to send Teams notification: {e}")


@dag(
    schedule="@weekly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=notify_teams,
    params={
        "dataset_id": Param(
            "", type="string", description="Specific Dataset ID (leave blank for all)"
        ),
        "overwrite": Param(
            False, type="boolean", description="Step 2: Overwrite all existing files"
        ),
    },
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["cheminformatics"],
)
def cheminformatics_pipeline():

    @task
    def find_and_filter_datasets(**context):
        params = context["params"]
        target_id = params.get("dataset_id", "").strip()
        overwrite = params.get("overwrite", False)
        last_launch = context["data_interval_start"]

        s3 = get_s3_client()
        bucket = "pipeline-results"
        aws_conn = os.getenv("AIRFLOW_CONN_AWS_S3")

        try:
            objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        except Exception:
            raise AirflowSkipException("Could not read bucket or bucket is empty.")

        scaffolds, r_groups = {}, {}

        for obj in objects:
            key = obj["Key"]
            last_modified = obj["LastModified"]
            if key.endswith("_scaffolds.csv"):
                scaffolds[key.replace("_scaffolds.csv", "")] = last_modified
            elif key.endswith("_r_groups.csv"):
                r_groups[key.replace("_r_groups.csv", "")] = last_modified

        env_maps = []
        for base_id, scaffold_modified in scaffolds.items():
            if base_id not in r_groups:
                continue

            if target_id and base_id != target_id:
                continue

            if not overwrite:
                rgroup_modified = r_groups[base_id]
                if scaffold_modified < last_launch and rgroup_modified < last_launch:
                    continue

            env_maps.append(
                {
                    "BUCKET": bucket,
                    "SCAFFOLD_KEY": f"{base_id}_scaffolds.csv",
                    "RGROUP_KEY": f"{base_id}_r_groups.csv",
                    "GENERATED_KEY": f"{base_id}_generated.csv",
                    "PROPS_KEY": f"{base_id}_properties.csv",
                    "CLUSTERED_KEY": f"{base_id}_clustered.csv",
                    "GRAPH_KEY": f"{base_id}_graph.zip",
                    "AIRFLOW_CONN_AWS_S3": aws_conn,
                }
            )

        if not env_maps:
            raise AirflowSkipException("No new datasets require processing.")

        return env_maps

    @task(on_success_callback=notify_teams, on_failure_callback=notify_teams)
    def soda_data_quality_check(env_maps):

        s3 = get_s3_client()

        for env in env_maps:
            bucket = env["BUCKET"]
            props_key = env["PROPS_KEY"]

            try:
                response = s3.get_object(Bucket=bucket, Key=props_key)
                csv_bytes = response["Body"].read()

                df = pd.read_csv(io.BytesIO(csv_bytes))

                scan = Scan()
                scan.set_scan_definition_name(f"DQ Check for {props_key}")
                scan.set_data_source_name("minio_pandas")

                scan.add_pandas_dataframe(
                    dataset_name="properties", pandas_df=df, data_source_name="minio_pandas"
                )

                scan.add_sodacl_yaml_file("/opt/airflow/dags/soda/dq_check.yml")

                scan.execute()

                if scan.has_check_fails():
                    print(scan.get_logs_text())
                    raise ValueError(f"Soda DQ Failed for {props_key}. See logs for details.")

            except Exception as e:
                raise ValueError(f"DQ task crashed on {props_key}: {str(e)}")

    env_maps = find_and_filter_datasets()

    generate = BashOperator.partial(
        task_id="generate_molecules",
        bash_command="PYTHONPATH=/opt/airflow/dags python /opt/airflow/dags/lib/tasks/generate_molecules.py --bucket $BUCKET --scaffold-key $SCAFFOLD_KEY --rgroup-key $RGROUP_KEY --output-key $GENERATED_KEY",
    ).expand(env=env_maps)

    calculate = BashOperator.partial(
        task_id="calculate_properties",
        bash_command="PYTHONPATH=/opt/airflow/dags python /opt/airflow/dags/lib/tasks/calculate_properties.py --bucket $BUCKET --input-key $GENERATED_KEY --output-key $PROPS_KEY",
    ).expand(env=env_maps)

    cluster = BashOperator.partial(
        task_id="cluster_molecules",
        bash_command="PYTHONPATH=/opt/airflow/dags python /opt/airflow/dags/lib/tasks/cluster_molecules.py --bucket $BUCKET --input-key $PROPS_KEY --output-key $CLUSTERED_KEY",
    ).expand(env=env_maps)

    graph = BashOperator.partial(
        task_id="build_faerun_graph",
        bash_command="PYTHONPATH=/opt/airflow/dags python /opt/airflow/dags/lib/tasks/build_faerun_graph.py --bucket $BUCKET --input-key $PROPS_KEY --output-key $GRAPH_KEY",
    ).expand(env=env_maps)

    dq_check = soda_data_quality_check(env_maps)
    generate >> calculate >> cluster >> graph >> dq_check


cheminformatics_pipeline()
