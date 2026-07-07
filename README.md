# airflow_task

-- Cheminformatics Airflow Pipeline
An automated, Dockerized orchestration workflow that processes MinIO/S3-hosted molecule datasets through generation, property calculation, clustering, and graph building. It features incremental processing, isolated Bash execution shells, native Soda data quality checks (via Pandas), and MS Teams integration.

1. Prerequisites
Ensure you have Docker and Docker Compose installed.

Initialize your environment variables by copying the example file:

cp .env.example .env
Open the new .env file and insert your actual MS Teams webhook URL so the pipeline can send failure alerts to the scientists:

2. Boot the Infrastructure
Run this command to build the custom Python dependencies and start the network in the background:


docker compose up --build -d
Note: The webserver requires approximately 60-90 seconds to fully install the data science libraries before exposing port 8080.

3. Access & Credentials
Once booted, access the Airflow UI at: http://localhost:8080

Username: admin

Password: Retrieve the dynamically generated password by running this command in your terminal:

docker compose exec airflow-webserver cat /opt/airflow/simple_auth_manager_passwords.json.generated

4. Pipeline Execution

The pipeline is scheduled to run @weekly but can be triggered manually via the Airflow UI using Trigger DAG w/ config:

dataset_id: Leave blank to process all datasets in the bucket, or enter a specific base ID to target a single batch.

overwrite: Defaults to False (incremental processing of new S3 files only). Toggle to True to bypass the time filter and forcefully reprocess historical data.