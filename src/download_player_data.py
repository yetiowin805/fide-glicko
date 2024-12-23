import os
import requests
import zipfile
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download FIDE player information.")
    parser.add_argument(
        "--data_dir",
        type=str,
        help="Base directory for all data files",
        default=".",
    )
    parser.add_argument(
        "--month",
        type=str,
        help="Month for the download in YYYY-MM format",
        required=True,
    )

    args = parser.parse_args()

    year, month = map(int, args.month.split("-"))

    BASE_URL = "http://ratings.fide.com/download/"

    month_mappings = {
        1: "jan",
        2: "feb",
        3: "mar",
        4: "apr",
        5: "may",
        6: "jun",
        7: "jul",
        8: "aug",
        9: "sep",
        10: "oct",
        11: "nov",
        12: "dec",
    }

    SAVE_PATH = os.path.join(args.data_dir, "player_info", "raw")
    if not os.path.exists(SAVE_PATH):
        os.makedirs(SAVE_PATH)

    month_str = month_mappings[month]
    year_str = str(year)[2:]

    expected_txt_file = os.path.join(SAVE_PATH, f"{year}-{month:02}.txt")
    if os.path.exists(expected_txt_file):
        print(f"File {expected_txt_file} already exists.")
    else:
        if year > 2012 or (year == 2012 and month >= 9):
            zip_header = f"standard_{month_str}{year_str}"
        else:
            zip_header = f"{month_str}{year_str}"

        url = BASE_URL + f"{zip_header}frl.zip"
        response = requests.get(url)
        zip_path = os.path.join(SAVE_PATH, f"{zip_header}frl.zip")

        with open(zip_path, "wb") as file:
            file.write(response.content)

        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(SAVE_PATH)
                os.rename(
                    os.path.join(SAVE_PATH, f"{zip_header}frl.txt"), expected_txt_file
                )
        except Exception as e:
            print(f"Failed to extract {zip_path}: {e}")

        os.remove(zip_path)

        print("Files downloaded, extracted, and renamed successfully!")
