#!/usr/bin/env python3
"""Script to upload existing data to AWS S3 bucket."""

import argparse
import os
from data_manager import DataManager
from config import LOCAL_DIRS
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def upload_data_to_s3(root_dir, path, dry_run=False):
    """Upload specified directory or file to S3.
    
    Args:
        root_dir (str): Root directory containing the data (e.g., 'data/')
        path (str): Relative path to upload
        dry_run (bool): If True, only preview what would be uploaded
    """
    data_manager = DataManager(use_s3=True)
    
    full_path = os.path.join(root_dir, path)
    
    if os.path.exists(full_path):
        if dry_run:
            logger.info(f"Would upload: {full_path}")
        else:
            logger.info(f"Uploading: {full_path}")
            success = data_manager.save_directory(full_path) if os.path.isdir(full_path) \
                else data_manager.save_file(full_path)
            if success:
                logger.info(f"Successfully uploaded: {full_path}")
            else:
                logger.error(f"Failed to upload: {full_path}")
    else:
        logger.warning(f"Path not found: {full_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload specified paths to S3")
    parser.add_argument(
        "--root-dir",
        type=str,
        default="data/",
        help="Root directory containing the data"
    )
    parser.add_argument(
        "--path",
        type=str,
        required=True,
        help="Path to upload (relative to root directory)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be uploaded without performing the upload"
    )
    
    args = parser.parse_args()
    upload_data_to_s3(args.root_dir, args.path, args.dry_run)