import boto3
import os

def upload_to_s3(local_file: str, bucket_name: str, s3_file: str):

    try:
        # Initialize S3 client
        s3_client = boto3.client("s3")
        
        # Upload file to S3
        s3_client.upload_file(local_file, bucket_name, s3_file)
        print(f"File uploaded successfully to s3://{bucket_name}/{s3_file}")
        
        # Verify the file exists
        response = s3_client.head_object(Bucket=bucket_name, Key=s3_file)
        print(f"Uploaded file metadata: {response}")
        
    except Exception as e:
        print(f"Error uploading file: {e}")
        raise

def delete_from_s3(bucket_name: str, s3_file: str):
    """
    Deletes a file from an S3 bucket.
    """
    try:
        # Initialize S3 client
        s3_client = boto3.client("s3")
        
        # Delete file from S3
        s3_client.delete_object(Bucket=bucket_name, Key=s3_file)
        print(f"File deleted successfully from s3://{bucket_name}/{s3_file}")
        
    except Exception as e:
        print(f"Error deleting file: {e}")
        raise


# Testing the upload and delete functionality
if __name__ == "__main__":
    bucket_name = "my-prefect-s3-bucket"
    local_file = "e_commerce_dataset.csv"
    s3_file = "prefect_pipeline_data/e_commerce.csv"
    

    #Upload the file to S3
    upload_to_s3(local_file, bucket_name, s3_file)
    
    # Optionally, delete the file to clean up (use this after testing)
    # delete_from_s3(bucket_name, s3_file)
