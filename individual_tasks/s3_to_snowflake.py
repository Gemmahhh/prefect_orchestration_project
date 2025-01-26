import boto3
import json
import snowflake.connector

def get_snowflake_credentials():
    """
    Fetch Snowflake credentials from AWS Systems Manager Parameter Store.
    """
    # Initialize SSM client
    ssm_client = boto3.client("ssm", region_name="eu-north-1") 

    # Fetch parameter
    parameter = ssm_client.get_parameter(
        Name="/myapp/snowflake/credentials",
        WithDecryption=True
    )

    # Parse and return credentials
    return json.loads(parameter["Parameter"]["Value"])

def load_to_snowflake(s3_path: str):
    """
    Load data from S3 to Snowflake using the COPY INTO command.
    """
    # Fetch credentials
    credentials = get_snowflake_credentials()

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=credentials["user"],
        password=credentials["password"],
        account=credentials["account"],
        warehouse=credentials["warehouse"],
        database=credentials["database"],
        schema=credentials["schema"]
    )

    # Query to copy data from S3
    query = f"""
    COPY INTO e_commerce
    FROM '{s3_path}'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
    """

    # Execute query
    cursor = conn.cursor()
    cursor.execute(query)
    conn.close()
    print(f"Data loaded successfully from {s3_path} to Snowflake!")

if __name__ == "__main__":
    # Example usage
    s3_path = "s3://my-prefect-s3-bucket/prefect_pipeline_data/e_commerce.csv" 
    load_to_snowflake(s3_path)
