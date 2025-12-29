"""
S3 utility functions for listing objects, incremental file detection, and connection handling.
"""
import boto3
from datetime import datetime, timedelta
from typing import List, Optional, Dict
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)


class S3Utils:
    """Utility class for S3 operations."""
    
    def __init__(self, bucket_name: str, region: str = 'us-east-1', 
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None):
        """
        Initialize S3 client.
        
        Args:
            bucket_name: S3 bucket name
            region: AWS region
            aws_access_key_id: AWS access key (optional, can use IAM role)
            aws_secret_access_key: AWS secret key (optional, can use IAM role)
        """
        self.bucket_name = bucket_name
        self.region = region
        
        # Initialize S3 client
        if aws_access_key_id and aws_secret_access_key:
            self.s3_client = boto3.client(
                's3',
                region_name=region,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
        else:
            # Use default credentials (IAM role, environment variables, etc.)
            self.s3_client = boto3.client('s3', region_name=region)
        
        self.s3_resource = boto3.resource('s3', region_name=region)
    
    def list_objects(self, prefix: str, file_extensions: Optional[List[str]] = None) -> List[Dict]:
        """
        List objects in S3 bucket with given prefix.
        
        Args:
            prefix: S3 key prefix
            file_extensions: Optional list of file extensions to filter (e.g., ['.csv', '.json'])
            
        Returns:
            List of object metadata dictionaries
        """
        objects = []
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        
                        # Filter by file extension if specified
                        if file_extensions:
                            if not any(key.endswith(ext) for ext in file_extensions):
                                continue
                        
                        objects.append({
                            'key': key,
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'],
                            'etag': obj['ETag']
                        })
            
            logger.info(f"Found {len(objects)} objects with prefix '{prefix}'")
            return objects
        
        except ClientError as e:
            logger.error(f"Error listing S3 objects: {e}")
            raise
    
    def get_incremental_files(self, prefix: str, last_processed_time: datetime,
                             file_extensions: Optional[List[str]] = None) -> List[Dict]:
        """
        Get files modified after last processed time (incremental processing).
        
        Args:
            prefix: S3 key prefix
            last_processed_time: Datetime to filter files modified after
            file_extensions: Optional list of file extensions to filter
            
        Returns:
            List of object metadata dictionaries
        """
        all_objects = self.list_objects(prefix, file_extensions)
        
        incremental_files = [
            obj for obj in all_objects
            if obj['last_modified'] > last_processed_time
        ]
        
        logger.info(f"Found {len(incremental_files)} incremental files since {last_processed_time}")
        return incremental_files
    
    def get_files_by_date_range(self, prefix: str, start_date: datetime, end_date: datetime,
                                file_extensions: Optional[List[str]] = None) -> List[Dict]:
        """
        Get files modified within a date range.
        
        Args:
            prefix: S3 key prefix
            start_date: Start datetime
            end_date: End datetime
            file_extensions: Optional list of file extensions to filter
            
        Returns:
            List of object metadata dictionaries
        """
        all_objects = self.list_objects(prefix, file_extensions)
        
        date_range_files = [
            obj for obj in all_objects
            if start_date <= obj['last_modified'] <= end_date
        ]
        
        logger.info(f"Found {len(date_range_files)} files in date range {start_date} to {end_date}")
        return date_range_files
    
    def get_s3_path(self, key: str) -> str:
        """
        Get full S3 path for a key.
        
        Args:
            key: S3 object key
            
        Returns:
            Full S3 path (s3://bucket/key)
        """
        return f"s3://{self.bucket_name}/{key}"
    
    def check_object_exists(self, key: str) -> bool:
        """
        Check if an object exists in S3.
        
        Args:
            key: S3 object key
            
        Returns:
            True if object exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
    
    def get_object_size(self, key: str) -> int:
        """
        Get size of an S3 object in bytes.
        
        Args:
            key: S3 object key
            
        Returns:
            Object size in bytes
        """
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return response['ContentLength']
        except ClientError as e:
            logger.error(f"Error getting object size for {key}: {e}")
            raise

