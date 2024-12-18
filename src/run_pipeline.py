import argparse
import os
from datetime import datetime, date


def get_months_between(start_month_str, end_month_str):
    start_year, start_month = map(int, start_month_str.split("-"))
    end_year, end_month = map(int, end_month_str.split("-"))

    start_date = date(start_year, start_month, 1)
    end_date = date(end_year, end_month, 1)

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
        "--upload_to_s3",
        type=str,
        help="Upload results to S3 bucket, y/n",
        required=False,
        default="n",
    )

    # Parse arguments
    args = parser.parse_args()

    # Get list of all months between start and end
    months = get_months_between(args.start_month, args.end_month)

    # Process each month
    for month in months:
        print(f"\nProcessing month: {month}")

        if args.download_player_data == "y":
            print(
                f"python3 download_player_data.py --data_dir {args.data_dir} --month {month}"
            )
            os.system(
                f"python3 src/download_player_data.py --data_dir {args.data_dir} --month {month}"
            )

            print(
                f"python3 process_fide_rating_list.py --month {month} --data_dir {args.data_dir}"
            )
            os.system(
                f"python3 src/process_fide_rating_list.py --month {month} --data_dir {args.data_dir}"
            )

        if args.scrape_fide == "y":
            print(f"python3 fide_scraper.py --month {month} --data_dir {args.data_dir}")
            os.system(
                f"python3 src/fide_scraper.py --month {month} --data_dir {args.data_dir}"
            )

            print(
                f"python3 tournament_scraper.py --month {month} --data_dir {args.data_dir}"
            )
            os.system(
                f"python3 src/tournament_scraper.py --month {month} --data_dir {args.data_dir}"
            )

        print(
            f"python3 extract_tournament_data.py --month {month} --data_dir {args.data_dir}"
        )
        os.system(
            f"python3 src/extract_tournament_data.py --month {month} --data_dir {args.data_dir}"
        )

    # Calculate adjusted start and end months (one month earlier)
    start_year, start_month = map(int, args.start_month.split("-"))
    end_year, end_month = map(int, args.end_month.split("-"))

    # Adjust start month
    if start_month == 1:
        adj_start_year = start_year - 1
        adj_start_month = 12
    else:
        adj_start_year = start_year
        adj_start_month = start_month - 1

    # Adjust end month
    if end_month == 1:
        adj_end_year = end_year - 1
        adj_end_month = 12
    else:
        adj_end_year = end_year
        adj_end_month = end_month - 1

    adj_start_month_str = f"{adj_start_year:04d}-{adj_start_month:02d}"
    adj_end_month_str = f"{adj_end_year:04d}-{adj_end_month:02d}"

    # Run collect_player_data.py with data_dir
    print(
        f"python3 collect_player_data.py --start_month {adj_start_month_str} --end_month {adj_end_month_str} --data_dir {args.data_dir}"
    )
    os.system(
        f"python3 src/collect_player_data.py --start_month {adj_start_month_str} --end_month {adj_end_month_str} --data_dir {args.data_dir}"
    )

    # Get months between adjusted dates for remove_duplicates.py
    adj_months = get_months_between(adj_start_month_str, adj_end_month_str)
    for month in adj_months:
        clean_numerical_path = os.path.join(args.data_dir, "clean_numerical", month)
        print(f"python3 remove_duplicates.py --root_dir {clean_numerical_path}")
        os.system(f"python3 src/remove_duplicates.py --root_dir {clean_numerical_path}")

    # Run glicko with adjusted dates
    print(
        f"python3 run_glicko.py --start_month {adj_start_month_str} --end_month {adj_end_month_str} --data_dir {args.data_dir}"
    )
    os.system(
        f"python3 src/run_glicko.py --start_month {adj_start_month_str} --end_month {adj_end_month_str} --data_dir {args.data_dir}"
    )