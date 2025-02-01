from prefect_dbt.cloud import DbtCloudCredentials

dbt_credentials = DbtCloudCredentials.load("my-dbt-cloud-credentials")
print(dbt_credentials)


from prefect_dbt.cloud import DbtCloudCredentials

# Replace with your actual dbt Cloud API token
dbt_credentials = DbtCloudCredentials(
    account_id=123456,  # Replace with your dbt Cloud account ID
    api_key="your-dbt-cloud-api-key"  # Replace with your dbt API Key
)

# Save the credentials as a Prefect block
dbt_credentials.save("my-dbt-cloud-credentials2", overwrite=True)
