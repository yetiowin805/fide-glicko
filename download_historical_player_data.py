import os
import requests
import zipfile
import re

BASE_URL = "http://ratings.fide.com/download/"
SAVE_PATH = "./player_info/"

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

# Ensure the save path exists
if not os.path.exists(SAVE_PATH):
    os.makedirs(SAVE_PATH)

# Generate URLs and download them
for year in range(2007, 2024):
    for month in range(1, 13):
        if year == 2007 and month < 11:
            continue
        if year == 2023 and month > 11:
            break

        month_str = month_mappings[month]
        year_str = str(year)[2:]

        # Check if the txt file already exists
        expected_txt_file = os.path.join(SAVE_PATH, f"{year}-{month:02}.txt")
        if os.path.exists(expected_txt_file):
            print(f"File {expected_txt_file} already exists. Skipping download for {month}/{year}.")
            continue

        if year > 2012 or (year == 2012 and month >= 9):
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
        except Exception:
            pass

        # Delete the zip file
        os.remove(zip_path)

# Rename files
for file_name in os.listdir(SAVE_PATH):
    # Check if the filename matches the expected format
    if re.match(r'standard_[a-z]{3}\d{2}frl\.txt', file_name):
        year = "20" + file_name[12:14]
        month = [num for num, abbr in month_mappings.items() if abbr == file_name[9:12]][0]
        new_file_name = f"{year}-{month:02}.txt"
        os.rename(os.path.join(SAVE_PATH, file_name), os.path.join(SAVE_PATH, new_file_name))
    elif re.match(r'[a-z]{3}\d{2}frl\.txt', file_name):
        year = "20" + file_name[3:5]
        month = [num for num, abbr in month_mappings.items() if abbr == file_name[0:3]][0]
        new_file_name = f"{year}-{month:02}.txt"
        os.rename(os.path.join(SAVE_PATH, file_name), os.path.join(SAVE_PATH, new_file_name))

print("Files downloaded, extracted, and renamed successfully!")
