import argparse
import os

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

    # Parse start and end month/year
    start_year, start_month = map(int, args.start_month.split("-"))
    end_year, end_month = map(int, args.end_month.split("-"))

    SAVE_PATH = "./player_info/raw"

    # run pipeline commands with start and end months
    print("Running pipeline commands...")
    if args.download_data == "y":
        print(f"python3 download_player_data.py --save_path {SAVE_PATH} --start_month {args.start_month} --end_month {args.end_month}")
        os.system(
            f"python3 download_player_data.py --save_path {SAVE_PATH} --start_month {args.start_month} --end_month {args.end_month}"
        )

        print(
            f"python3 fide_scraper.py --start_month {args.start_month} --end_month {args.end_month}"
        )
        os.system(
            f"python3 fide_scraper.py --start_month {args.start_month} --end_month {args.end_month}"
        )

        print(
            f"python3 tournament_scraper.py --start_month {args.start_month} --end_month {args.end_month}"
        )
        os.system(
            f"python3 tournament_scraper.py --start_month {args.start_month} --end_month {args.end_month}"
        )

    print(f"python3 extract_tournament_data.py --start_month {args.start_month} --end_month {args.end_month}")
    os.system(f"python3 extract_tournament_data.py --start_month {args.start_month} --end_month {args.end_month}")

    # Shift start month and end month back one month
    start_month = start_month - 1
    end_month = end_month - 1

    # Adjust start year and end year if necessary
    if start_month == 0:
        start_month = 12
        start_year -= 1

    if end_month == 0:
        end_month = 12
        end_year -= 1

    args.start_month = f"{start_year:04d}-{start_month:02d}"
    args.end_month = f"{end_year:04d}-{end_month:02d}"

    # loop through months and call remove_duplicates on folder
    for year in range(start_year, end_year + 1):
        for month in range(
            start_month if year == start_year else 1,
            end_month + 1 if year == end_year else 13,
        ):
            print(
                f"python3 collect_player_data.py --start_month {year:04d}-{month:02d} --end_month {year:04d}-{month:02d}"
            )
            os.system(
                f"python3 collect_player_data.py --start_month {year:04d}-{month:02d} --end_month {year:04d}-{month:02d}"
            )

            print(
                f"python3 remove_duplicates.py --root_dir ./clean_numerical/{year:04d}-{month:02d}"
            )
            os.system(
                f"python3 remove_duplicates.py --root_dir ./clean_numerical/{year:04d}-{month:02d}"
            )

            print(
                f"python3 run_glicko.py --start_month {year:04d}-{month:02d} --end_month {year:04d}-{month:02d}"
            )
            os.system(
                f"python3 run_glicko.py --start_month {year:04d}-{month:02d} --end_month {year:04d}-{month:02d}"
            )
