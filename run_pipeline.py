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
        "--download_data",
        type=str,
        help="Determines whether to download data first, y/n",
        required=False,
        default="y",
    )

    # Parse arguments
    args = parser.parse_args()
    SAVE_PATH = "./player_info/raw"

    # Get list of all months between start and end
    months = get_months_between(args.start_month, args.end_month)
    
    # Currently, this is the best way to do this. It looks dumb, and the downstream commands should be modified
    # to be called for just one month. In the future, we should default to a pipeline and works for modern data,
    # and create fallback code in case we wish to re-run on old data. Or maybe not, that way is not really faster, to be honest

    # Process each month
    for month in months:
        print(f"\nProcessing month: {month}")
        
        if args.download_data == "y":
            print(f"python3 download_player_data.py --save_path {SAVE_PATH} --start_month {month} --end_month {month}")
            os.system(f"python3 download_player_data.py --save_path {SAVE_PATH} --start_month {month} --end_month {month}")

            print(f"python3 process_fide_rating_list.py --start_month {month} --end_month {month}")
            os.system(f"python3 process_fide_rating_list.py --start_month {month} --end_month {month}")

            print(f"python3 fide_scraper.py --start_month {month} --end_month {month}")
            os.system(f"python3 fide_scraper.py --start_month {month} --end_month {month}")

            print(f"python3 tournament_scraper.py --start_month {month} --end_month {month}")
            os.system(f"python3 tournament_scraper.py --start_month {month} --end_month {month}")

        print(f"python3 extract_tournament_data.py --start_month {month} --end_month {month}")
        os.system(f"python3 extract_tournament_data.py --start_month {month} --end_month {month}")

        # Get previous month for collect_player_data
        year, month_num = map(int, month.split("-"))
        if month_num == 1:
            prev_year = year - 1
            prev_month = 12
        else:
            prev_year = year
            prev_month = month_num - 1
        prev_month_str = f"{prev_year:04d}-{prev_month:02d}"

        print(f"python3 collect_player_data.py --start_month {prev_month_str} --end_month {prev_month_str}")
        os.system(f"python3 collect_player_data.py --start_month {prev_month_str} --end_month {prev_month_str}")

        print(f"python3 remove_duplicates.py --root_dir ./clean_numerical/{prev_month_str}")
        os.system(f"python3 remove_duplicates.py --root_dir ./clean_numerical/{prev_month_str}")

        print(f"python3 run_glicko.py --start_month {prev_month_str} --end_month {prev_month_str}")
        os.system(f"python3 run_glicko.py --start_month {prev_month_str} --end_month {prev_month_str}")