import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from lib.utils.aws import get_s3_client

@dag(
    schedule="@weekly",  # Step 2: Weekly schedule
    start_date=datetime(2026, 6, 6),
    catchup=False,
    params={
        "dataset_id": Param("", type="string", description="Specific Dataset ID (leave blank for all)"),
        "overwrite": Param(False, type="boolean", description="Step 2: Overwrite all existing files"),
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

        scaffolds = {}
        r_groups = {}

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
                continue  # Skip if missing r_groups pair

            if target_id and base_id != target_id:
                continue  # Filter by specific ID

            # If overwrite is False, ONLY process files that appeared since the last launch
            if not overwrite:
                rgroup_modified = r_groups[base_id]
                if scaffold_modified < last_launch and rgroup_modified < last_launch:
                    continue

            # Map arguments for BashOperator
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

    generate >> calculate >> cluster >> graph

cheminformatics_pipeline()