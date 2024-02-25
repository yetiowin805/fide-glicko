import os
import requests
import zipfile
import re
import argparse

# This is the first command in the pipeline

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Download FIDE player information.')
    parser.add_argument('--save_path', type=str, help='Path to save the downloaded files', required=True)
    parser.add_argument('--start_month', type=str, help='Start month for the download in YYYY-MM format', required=True)
    parser.add_argument('--end_month', type=str, help='End month for the download in YYYY-MM format', required=True)

    # Parse arguments
    args = parser.parse_args()

    # Parse start and end month/year
    start_year, start_month = map(int, args.start_month.split('-'))
    end_year, end_month = map(int, args.end_month.split('-'))

    BASE_URL = "http://ratings.fide.com/download/"

    month_mappings = {
        1: 'jan',
        2: 'feb',
        3: 'mar',
        4: 'apr',
        5: 'may',
        6: 'jun',
        7: 'jul',
        8: 'aug',
        9: 'sep',
        10: 'oct',
        11: 'nov',
        12: 'dec'
    }

    # Use the save path from arguments
    SAVE_PATH = args.save_path

    # Ensure the save path exists
    if not os.path.exists(SAVE_PATH):
        os.makedirs(SAVE_PATH)

    # Adjust loop to iterate from start_month/start_year to end_month/end_year
    current_year, current_month = start_year, start_month

    while current_year < end_year or (current_year == end_year and current_month <= end_month):
        month_str = month_mappings[current_month]
        year_str = str(current_year)[2:]

        # Check if the txt file already exists
        expected_txt_file = os.path.join(SAVE_PATH, f"{current_year}-{current_month:02}.txt")
        if os.path.exists(expected_txt_file):
            print(f"File {expected_txt_file} already exists. Skipping download for {current_month}/{current_year}.")
        else:
            if current_year > 2012 or (current_year == 2012 and current_month >= 9):
                zip_header = f"standard_{month_str}{year_str}"
            else:
                zip_header = f"{month_str}{year_str}"

            # Generate the URL
            url = BASE_URL + f"{zip_header}frl.zip"

            # Download the zip file
            response = requests.get(url)
            zip_path = os.path.join(SAVE_PATH, f"{zip_header}frl.zip")
            
            with open(zip_path, 'wb') as file:
                file.write(response.content)

            # Extract the zip file
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(SAVE_PATH)
            except Exception as e:
                print(f"Failed to extract {zip_path}: {e}")

            # Delete the zip file
            os.remove(zip_path)

        # Increment month/year
        if current_month == 12:
            current_year += 1
            current_month = 1
        else:
            current_month += 1

    # Rename files (No changes needed here)

    print("Files downloaded, extracted, and renamed successfully!")
