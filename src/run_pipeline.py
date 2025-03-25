import argparse
import os
from datetime import datetime, date
import subprocess


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
        "--upload_to_s3",
        type=str,
        help="Upload results to S3 bucket, y/n",
        required=False,
        default="n",
    )

    # Parse arguments
    args = parser.parse_args()

    # Convert string arguments to date objects before calling function
    start_year, start_month = map(int, args.start_month.split("-"))
    end_year, end_month = map(int, args.end_month.split("-"))
    start_date = date(start_year, start_month, 1)
    end_date = date(end_year, end_month, 1)

    # Get list of all months between start and end
    months = get_months_between(start_date, end_date)

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

            if args.upload_to_s3 == "y":
                print(
                    f"python3 upload_to_s3.py --data_dir {args.data_dir} --path player_info/processed/{month}"
                )
                os.system(
                    f"python3 src/aws/upload_to_s3.py --data_dir {args.data_dir} --path player_info/processed/{month}"
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

    # # # Calculate adjusted dates (one month earlier)
    # if start_date.month == 1:
    #     adj_start_date = date(start_date.year - 1, 12, 1)
    # else:
    #     adj_start_date = date(start_date.year, start_date.month - 1, 1)

    # if end_date.month == 1:
    #     adj_end_date = date(end_date.year - 1, 12, 1)
    # else:
    #     adj_end_date = date(end_date.year, end_date.month - 1, 1)

    # # # Get months between adjusted dates for remove_duplicates.py
    # adj_months = get_months_between(adj_start_date, adj_end_date)
    # for month in adj_months:
    #     current_year = int(month.split("-")[0])
    #     print(
    #         f"python3 collect_player_data.py --month {month} --time_control Standard --data_dir {args.data_dir}"
    #     )
    #     os.system(
    #         f"python3 src/collect_player_data.py --month {month} --time_control Standard --data_dir {args.data_dir}"
    #     )
    #     if current_year >= 2012:
    #         for time_control in ["Rapid", "Blitz"]:
    #             print(
    #                 f"python3 collect_player_data.py --month {month} --time_control {time_control} --data_dir {args.data_dir}"
    #             )
    #             os.system(
    #                 f"python3 src/collect_player_data.py --month {month} --time_control {time_control} --data_dir {args.data_dir}"
    #             )
    #     clean_numerical_path = os.path.join(args.data_dir, "clean_numerical", month)
    #     print(f"python3 remove_duplicates.py --root_dir {clean_numerical_path}")
    #     os.system(f"python3 src/remove_duplicates.py --root_dir {clean_numerical_path}")

    # if args.upload_to_s3 == "y":
    #     # Upload each month's data to S3
    #     for month in adj_months:
    #         print(
    #             f"python3 upload_to_s3.py --data_dir {args.data_dir} --path clean_numerical/{month}"
    #         )
    #         os.system(
    #             f"python3 src/aws/upload_to_s3.py --data_dir {args.data_dir} --path clean_numerical/{month}"
    #         )

    # # When calling run_glicko.py, format the date without the day
    # adj_start_month_str = f"{adj_start_date.year:04d}-{adj_start_date.month:02d}"
    # adj_end_month_str = f"{adj_end_date.year:04d}-{adj_end_date.month:02d}"

    # # Call run_glicko.py with correct date format
    # subprocess.run(
    #     [
    #         "python3",
    #         "src/run_glicko.py",
    #         "--start_month",
    #         adj_start_month_str,  # YYYY-MM format
    #         "--end_month",
    #         adj_end_month_str,  # YYYY-MM format
    #         "--data_dir",
    #         args.data_dir,
    #         "--upload_to_s3",
    #         args.upload_to_s3,
    #     ]
    # )
