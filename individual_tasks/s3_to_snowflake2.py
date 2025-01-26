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

def load_to_snowflake(stage_path: str):
    """
    Load data from a Snowflake stage to a Snowflake table using the COPY INTO command.
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

    # Fully qualify the table name
    table_name = f"{credentials['database']}.{credentials['schema']}.e_commerce"

    # Query to copy data from the stage
    query = f"""
    COPY INTO {table_name}
    FROM '{stage_path}'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
    """  # Use the stage path directly

    # Execute query
    cursor = conn.cursor()
    cursor.execute(query)
    conn.close()
    print(f"Data loaded successfully from stage path '{stage_path}' to Snowflake table '{table_name}'!")

if __name__ == "__main__":
    # Example usage
    stage_path = "@my_s3_stage/e_commerce.csv"  
    load_to_snowflake(stage_path)
