"""Data manager for handling both local and S3 storage."""

import os
from s3_utils import S3Handler
from config import S3_CONFIG, LOCAL_DIRS
import logging

logger = logging.getLogger(__name__)

class DataManager:
    def __init__(self, use_s3=True):
        """
        Initialize the data manager.
        
        Args:
            use_s3 (bool): Whether to use S3 storage in addition to local storage
        """
        self.use_s3 = use_s3
        if use_s3:
            self.s3_handler = S3Handler(
                bucket_name=S3_CONFIG['bucket_name'],
                region_name=S3_CONFIG['region_name']
            )
        
        # Create local directories if they don't exist
        for directory in LOCAL_DIRS.values():
            os.makedirs(directory, exist_ok=True)

    def save_file(self, local_path, remote_path=None):
        """
        Save a file locally and optionally to S3.
        
        Args:
            local_path (str): Path to the local file
            remote_path (str): S3 key for the file. If None, derives from local path
        """
        if not os.path.exists(local_path):
            logger.error(f"Local file not found: {local_path}")
            return False

        if self.use_s3:
            if remote_path is None:
                # Convert local path to S3 path
                remote_path = self._local_to_s3_path(local_path)
            return self.s3_handler.upload_file(local_path, remote_path)
        return True

    def load_file(self, local_path, remote_path=None):
        """
        Load a file from S3 to local storage if using S3,
        or verify local file exists.
        
        Args:
            local_path (str): Path where the file should be stored locally
            remote_path (str): S3 key for the file. If None, derives from local path
        """
        if self.use_s3:
            if remote_path is None:
                remote_path = self._local_to_s3_path(local_path)
            return self.s3_handler.download_file(remote_path, local_path)
        return os.path.exists(local_path)

    def save_directory(self, local_dir, remote_prefix=None):
        """
        Save an entire directory locally and optionally to S3.
        
        Args:
            local_dir (str): Path to the local directory
            remote_prefix (str): S3 prefix for the directory
        """
        if not os.path.exists(local_dir):
            logger.error(f"Local directory not found: {local_dir}")
            return False

        if self.use_s3:
            if remote_prefix is None:
                remote_prefix = self._local_to_s3_path(local_dir)
            return self.s3_handler.upload_directory(local_dir, remote_prefix)
        return True

    def load_directory(self, local_dir, remote_prefix=None):
        """
        Load a directory from S3 to local storage if using S3,
        or verify local directory exists.
        
        Args:
            local_dir (str): Path where the directory should be stored locally
            remote_prefix (str): S3 prefix for the directory
        """
        if self.use_s3:
            if remote_prefix is None:
                remote_prefix = self._local_to_s3_path(local_dir)
            return self.s3_handler.download_directory(remote_prefix, local_dir)
        return os.path.exists(local_dir)

    def _local_to_s3_path(self, local_path):
        """Convert a local path to an S3 path."""
        # Get the base path name without any directory structure
        base_path = os.path.basename(os.path.dirname(local_path))
        
        # Find the matching directory in LOCAL_DIRS
        for dir_name, dir_path in LOCAL_DIRS.items():
            if base_path in dir_path:
                # Use the S3 directory structure from S3_CONFIG
                s3_prefix = S3_CONFIG['directories'][dir_name]
                # Get the relative path from the base directory
                rel_path = os.path.relpath(local_path, os.path.dirname(local_path))
                # Combine the S3 prefix with the relative path
                return os.path.join(s3_prefix, rel_path).replace('\\', '/')
                
        # If no match found, use the basename as the key
        return os.path.basename(local_path).replace('\\', '/')