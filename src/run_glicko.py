import os
import shutil
import argparse

# Sixth command in pipeline


def run_glicko(
    folder, start_year, start_month, end_year, end_month, data_dir, upload_to_s3=False
):
    for year in range(start_year, end_year + 1):
        # For the start year, use the provided start month. For other years, start from January.
        start_m = start_month if year == start_year else 1

        end_m = end_month if year == end_year else 12

        for month in range(start_m, end_m + 1):
            next_month = month + 1
            next_year = year

            # Handle year-end rollover
            if next_month > 12:
                next_month = 1
                next_year += 1

            player_info_path = (
                f"{data_dir}/player_info/processed/{next_year:04d}-{next_month:02d}.txt"
            )

            temp_year, temp_month = next_year, next_month
            while not os.path.exists(player_info_path):
                # If the player_info file for the current month doesn't exist, move to the previous month
                if temp_month == 1:
                    temp_year -= 1
                    temp_month = 12
                else:
                    temp_month -= 1
                player_info_path = f"{data_dir}/player_info/processed/{temp_year:04d}-{temp_month:02d}.txt"
                if temp_year < start_year - 1:
                    break
            if temp_year < start_year - 1:
                break

            cmd = (
                f"python3 {os.path.join(os.path.dirname(__file__), 'glicko2.py')} "
                f"{data_dir}/rating_lists/{folder}/{year:04d}-{month:02d}.txt "
                f"{data_dir}/clean_numerical/{year:04d}-{month:02d}/{folder}/games.txt "
                f"{data_dir}/rating_lists/{folder}/{next_year:04d}-{next_month:02d}.txt "
                f"{data_dir}/top_rating_lists/ "
                f"{folder}/{next_year:04d}-{next_month:02d} "
                f"{player_info_path} "
                f"{next_year:04d}"
            )

            print(cmd)

            os.system(cmd)


def main(start_year, start_month, end_year, end_month, data_dir, upload_to_s3=False):
    # Run for Standard
    run_glicko(
        "Standard", start_year, start_month, end_year, end_month, data_dir, upload_to_s3
    )

    if start_year <= 2011 and end_year >= 2012:
        # Copy the 2011-12 rating list to Rapid and Blitz
        src_file = f"{data_dir}/rating_lists/Standard/2011-12.txt"
        shutil.copy(src_file, f"{data_dir}/rating_lists/Rapid/2011-12.txt")
        shutil.copy(src_file, f"{data_dir}/rating_lists/Blitz/2011-12.txt")

        # Run for Rapid and Blitz starting from 2011-12
        run_glicko("Rapid", 2011, 12, end_year, end_month, data_dir, upload_to_s3)
        run_glicko("Blitz", 2011, 12, end_year, end_month, data_dir, upload_to_s3)
    elif start_year >= 2012:
        # Run for Rapid and Blitz
        run_glicko(
            "Rapid",
            start_year,
            start_month,
            end_year,
            end_month,
            data_dir,
            upload_to_s3,
        )
        run_glicko(
            "Blitz",
            start_year,
            start_month,
            end_year,
            end_month,
            data_dir,
            upload_to_s3,
        )


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Run glicko algorithm on selected months."
    )
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

    # Parse start and end month/year
    start_year, start_month = map(int, args.start_month.split("-"))
    end_year, end_month = map(int, args.end_month.split("-"))
    main(
        start_year,
        start_month,
        end_year,
        end_month,
        args.data_dir,
        args.upload_to_s3 == "y",
    )
