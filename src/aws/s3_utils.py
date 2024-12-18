import boto3
import os
from botocore.exceptions import ClientError, NoCredentialsError
import logging

logger = logging.getLogger(__name__)

class S3Handler:
    def __init__(self, bucket_name, region_name='us-east-2'):
        """Initialize S3 handler with bucket information."""
        self.bucket_name = bucket_name
        
        # Create session using default credentials (from ~/.aws/credentials)
        session = boto3.Session()
        self.s3_client = session.client('s3', region_name=region_name)
        
        try:
            # Test connection
            self.s3_client.head_bucket(Bucket=bucket_name)
            logger.info("Successfully connected to S3 bucket")
        except NoCredentialsError:
            logger.error("No AWS credentials found in ~/.aws/credentials")
            raise
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"Bucket {bucket_name} does not exist")
            elif error_code == '403':
                logger.error("Access denied. Please check your AWS credentials and permissions.")
            else:
                logger.error(f"AWS Error: {str(e)}")
            raise

    def upload_file(self, file_path, s3_key=None):
        """
        Upload a file to S3 bucket.
        
        Args:
            file_path (str): Local path to the file
            s3_key (str): The S3 key (path) where the file will be stored.
                         If not provided, uses the filename.
        
        Returns:
            bool: True if file was uploaded successfully, False otherwise
        """
        if s3_key is None:
            s3_key = os.path.basename(file_path)

        try:
            self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
            logger.info(f"Successfully uploaded {file_path} to {self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {file_path}: {str(e)}")
            return False

    def download_file(self, s3_key, local_path):
        """
        Download a file from S3 bucket.
        
        Args:
            s3_key (str): The S3 key (path) of the file to download
            local_path (str): Local path where the file should be saved
        
        Returns:
            bool: True if file was downloaded successfully, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            logger.info(f"Successfully downloaded {s3_key} to {local_path}")
            return True
        except ClientError as e:
            logger.error(f"Failed to download {s3_key}: {str(e)}")
            return False

    def upload_directory(self, directory_path, prefix=''):
        """
        Upload an entire directory to S3 bucket.
        
        Args:
            directory_path (str): Local directory path to upload
            prefix (str): Prefix to add to S3 keys (like a folder path)
        
        Returns:
            bool: True if all files were uploaded successfully, False if any failed
        """
        success = True
        for root, _, files in os.walk(directory_path):
            for file in files:
                local_path = os.path.join(root, file)
                # Create S3 key maintaining directory structure
                relative_path = os.path.relpath(local_path, directory_path)
                s3_key = os.path.join(prefix, relative_path).replace('\\', '/')
                
                if not self.upload_file(local_path, s3_key):
                    success = False
        
        return success

    def download_directory(self, prefix, local_directory):
        """
        Download an entire directory from S3 bucket.
        
        Args:
            prefix (str): S3 prefix (folder path) to download
            local_directory (str): Local directory where files should be saved
        
        Returns:
            bool: True if all files were downloaded successfully, False if any failed
        """
        try:
            # List all objects with the given prefix
            paginator = self.s3_client.get_paginator('list_objects_v2')
            success = True
            
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    s3_key = obj['Key']
                    # Create local path maintaining directory structure
                    relative_path = s3_key[len(prefix):].lstrip('/')
                    local_path = os.path.join(local_directory, relative_path)
                    
                    if not self.download_file(s3_key, local_path):
                        success = False
            
            return success
        except ClientError as e:
            logger.error(f"Failed to download directory {prefix}: {str(e)}")
            return False