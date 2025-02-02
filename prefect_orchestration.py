from prefect import flow, task
from prefect_aws.s3 import S3Bucket
from prefect_dbt.cloud import DbtCloudCredentials, DbtCloudJob
from prefect_snowflake.database import SnowflakeConnector
import boto3
from httpx import Timeout

# Task 1: Upload data to S3
@task
def upload_to_s3(local_file: str, s3_bucket_block_name: str, s3_file: str):
    """
    Uploads a file to S3 using a Prefect S3Bucket block.
    """
    s3_bucket = S3Bucket.load(s3_bucket_block_name)
    s3_bucket.upload_from_path(from_path=local_file, to_path=s3_file)
    print(f"File uploaded successfully to S3 path: {s3_file}")

# Task 2: Load data from S3 to Snowflake
@task
def load_to_snowflake(stage_path: str, snowflake_block_name: str, table_name: str):
    """
    Loads data from S3 into Snowflake using a Prefect SnowflakeConnector block.
    """
    snowflake_connector = SnowflakeConnector.load(snowflake_block_name)

    # SQL to copy data from S3 to Snowflake
    query = f"""
    COPY INTO {table_name}
    FROM '{stage_path}'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
    """

    with snowflake_connector.get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            print(f"Data loaded successfully into Snowflake table: {table_name}")

# Task 3: Trigger a dbt Cloud job
# @task
# def trigger_dbt_cloud_job(dbt_block_name: str, job_id: int):
#     """
#     Triggers a dbt Cloud job using a Prefect DbtCloudCredentials block.
#     """
#     dbt_credentials = DbtCloudCredentials.load(dbt_block_name)
#     dbt_job = DbtCloudJob(credentials=dbt_credentials, job_id=job_id)
#     dbt_job.run()
#     print(f"Triggered dbt Cloud job with Job ID: {job_id}")


@task(retries=3, retry_delay_seconds=10)
def trigger_dbt_cloud_job(dbt_block_name: str, job_id: int):
    """
    Triggers a dbt Cloud job using a Prefect DbtCloudCredentials block.
    """
    dbt_credentials = DbtCloudCredentials.load(dbt_block_name)
    print(f"Loaded dbt credentials: {dbt_credentials}")
    #dbt_job = DbtCloudJob(credentials=dbt_credentials, job_id=job_id)
    dbt_job = DbtCloudJob(dbt_cloud_credentials=dbt_credentials, job_id=job_id)
    dbt_job.trigger()    
    print(f"Triggered dbt Cloud job with Job ID: {job_id}")




# Prefect Flow
@flow(name="Data Pipeline Orchestration")
def data_pipeline_flow():
    """
    Orchestrates the entire pipeline:
    1. Upload data to S3.
    2. Load data from S3 to Snowflake.
    3. Trigger a dbt Cloud job.
    """
    # Parameters
    local_file = "e_commerce_dataset.csv"  
    s3_bucket_block_name = "my-s3-bucket-block"  
    # s3_file = "prefect_pipeline_data/e_commerce.csv"  
    s3_file = "e_commerce.csv"
    snowflake_block_name = "my-snowflake-connector"  
    table_name = "raw_schema.e_commerce"  
    # s3_path = f"s3://my-prefect-s3-bucket/{s3_file}"
    stage_path = f"@my_s3_stage/{s3_file}"  
    dbt_block_name = "my-dbt-cloud-credentials"  
    dbt_job_id = 70471823420051  

    
    # Step 1: Upload data to S3
    upload_to_s3(local_file, s3_bucket_block_name, s3_file)

    # Step 2: Load data from S3 to Snowflake
    load_to_snowflake(stage_path, snowflake_block_name, table_name)

    # Step 3: Trigger a dbt Cloud job for transformations
    trigger_dbt_cloud_job(dbt_block_name, dbt_job_id)
    #test purpose
# Run the flow
if __name__ == "__main__":
    data_pipeline_flow()
