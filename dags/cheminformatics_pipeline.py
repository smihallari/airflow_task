from datetime import datetime, timedelta
import os

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from lib.utils.aws import get_s3_client


@dag(
    schedule=None,
    start_date=datetime(2026, 6, 6),
    catchup=False,
    params={
        "dataset_id": Param(
            "", type="string", description="Specific Dataset ID (leave blank for all)"
        ),
    },
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["cheminformatics"],
)
def cheminformatics_pipeline():

    @task
    def find_and_filter_datasets(**context):
        """Identifies matched file pairs in MinIO and applies overwrite/time filtering."""
        params = context["params"]
        target_id = params.get("dataset_id", "").strip()
        aws_conn = os.getenv("AIRFLOW_CONN_AWS_S3")
        s3 = get_s3_client()
        bucket = "pipeline-results"

        try:
            objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        except Exception:
            raise AirflowSkipException("Could not read bucket or bucket is empty.")

        (scaffolds, r_groups) = (set(), set())

        # Parse bucket contents
        for obj in objects:
            key = obj["Key"]
            if key.endswith("_scaffolds.csv"):
                scaffolds.add(key.replace("_scaffolds.csv", ""))
            elif key.endswith("_r_groups.csv"):
                r_groups.add(key.replace("_r_groups.csv", ""))

        env_maps = []
        for base_id in scaffolds:
            if base_id not in r_groups:
                continue  # Skip if missing r_groups pair

            if target_id and base_id != target_id:
                continue  # Filter by specific ID

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
bash_command="PYTHONPATH=/opt/airflow/dags python /opt/airflow/dags/lib/tasks/generate_molecules.py --bucket $BUCKET --scaffold-key $SCAFFOLD_KEY --rgroup-key $RGROUP_KEY --output-key $GENERATED_KEY",    ).expand(env=env_maps)

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
