import argparse
import os
import gc
from datetime import datetime, date
import subprocess
import logging


def get_months_between(start_date: date, end_date: date) -> list[str]:
    """Return list of month strings (YYYY-MM) between two dates, inclusive."""
    months = []
    current_date = start_date
    while current_date <= end_date:
        months.append(f"{current_date.year:04d}-{current_date.month:02d}")
        # Move to next month
        if current_date.month == 12:
            current_date = date(current_date.year + 1, 1, 1)
        else:
            current_date = date(current_date.year, current_date.month + 1, 1)
    return months


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Runs entire pipeline")
    parser.add_argument(
        "--start_month",
        type=str,
        help="Start month for the download in YYYY-MM format",
        required=True,
    )
    parser.add_argument(
        "--end_month",
        type=str,
        help="End month for the download in YYYY-MM format",
        required=True,
    )
    parser.add_argument(
        "--download_player_data",
        type=str,
        help="Download and process FIDE rating lists, y/n",
        required=False,
        default="n",
    )
    parser.add_argument(
        "--scrape_fide",
        type=str,
        help="Scrape FIDE website for player and tournament data, y/n",
        required=False,
        default="y",
    )
    parser.add_argument(
        "--data_dir",
        type=str,
        help="Base directory for all data files",
        default=".",
    )
    parser.add_argument(
        "--force_gc",
        type=str,
        help="Force garbage collection between months, y/n",
        default="y",
    )
    parser.add_argument(
        "--log_level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level",
        default="INFO",
    )

    # Parse arguments
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Convert string arguments to date objects before calling function
    start_year, start_month = map(int, args.start_month.split("-"))
    end_year, end_month = map(int, args.end_month.split("-"))
    start_date = date(start_year, start_month, 1)
    end_date = date(end_year, end_month, 1)

    # Get list of all months between start and end
    months = get_months_between(start_date, end_date)

    # Process each month
    for i, month in enumerate(months):
        logging.info(f"\n{'='*50}\nProcessing month: {month} ({i+1}/{len(months)})\n{'='*50}")
        
        # Dictionary to store memory usage info
        memory_info = {}
        
        try:
            if args.download_player_data == "y":
                cmd = f"python3 src/download_player_data.py --data_dir {args.data_dir} --month {month}"
                logging.info(f"Running: {cmd}")
                subprocess.run(cmd, shell=True, check=True)
                
                cmd = f"python3 src/process_fide_rating_list.py --month {month} --data_dir {args.data_dir}"
                logging.info(f"Running: {cmd}")
                subprocess.run(cmd, shell=True, check=True)

            if args.scrape_fide == "y":
                cmd = f"python3 src/fide_scraper.py --month {month} --data_dir {args.data_dir}"
                logging.info(f"Running: {cmd}")
                subprocess.run(cmd, shell=True, check=True)
                
                cmd = f"python3 src/tournament_scraper.py --month {month} --data_dir {args.data_dir}"
                logging.info(f"Running: {cmd}")
                subprocess.run(cmd, shell=True, check=True)
                
                cmd = f"python3 src/aggregate_player_ids.py --month {month} --data_dir {args.data_dir}"
                logging.info(f"Running: {cmd}")
                subprocess.run(cmd, shell=True, check=True)
                
                cmd = f"python3 src/scrape_calculations.py --month {month} --data_dir {args.data_dir}"
                logging.info(f"Running: {cmd}")
                subprocess.run(cmd, shell=True, check=True)

            # Run process_game_data.py to convert to clean numerical format
            cmd = f"python3 src/process_game_data.py --month {month} --data_dir {args.data_dir}"
            logging.info(f"Running: {cmd}")
            subprocess.run(cmd, shell=True, check=True)
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Error processing month {month}: {e}")
        
        # Perform garbage collection to free memory between months
        if args.force_gc == "y":
            before_count = gc.get_count()
            logging.info(f"Running garbage collection... (before: {before_count})")
            n_collected = gc.collect(generation=2)  # Full collection
            after_count = gc.get_count()
            logging.info(f"Garbage collection completed: {n_collected} objects collected. (after: {after_count})")
        
        logging.info(f"Finished processing month: {month}\n")
        
    logging.info("Pipeline completed successfully!")
