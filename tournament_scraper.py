import os
import requests
from bs4 import BeautifulSoup
import re
from multiprocessing import Pool
import argparse
from countries import countries

# Third command in pipeline


def scrape_tournament_data(country, month, year):
    # Pad the month with a leading zero if it's less than 10
    month_str = f"{month:02d}"
    # Create the formatted string
    formatted_str = f"{year}-{month_str}"
    # Create the path
    path = os.path.join(
        "raw_tournament_data", country, formatted_str, "tournaments.txt"
    )

    # Check if the file exists
    if os.path.isfile(path):
        print(path)
        with open(path, "r") as f:
            lines = f.readlines()

        # Define base URL
        base_url = "https://ratings.fide.com/"

        # Loop through each line in the file
        for line in lines:
            # Extract the code from the line
            code = line[:-1]

            # Create the file path for the new file
            new_path = os.path.join(os.path.dirname(path), "info", f"{code}.txt")

            # Check if the new file path exists and is not empty
            if os.path.exists(new_path) and os.path.getsize(new_path) > 0:
                # File exists and is not empty, skip this iteration
                continue

            # If the file doesn't exist or is empty, fetch data from the URL
            # Form the complete URL
            url = base_url + "tournament_details.phtml?event=" + code

            # Make the HTTP request
            response = requests.get(url)

            # Parse the HTML content
            soup = BeautifulSoup(response.text, "html.parser")

            # Make sure the directory exists
            os.makedirs(os.path.dirname(new_path), exist_ok=True)

            # Write the contents of 'soup' into the file
            with open(new_path, "w", encoding="utf-8") as f:
                f.write(str(soup))

        # Loop through each line in the file
        for line in lines:
            # Extract the code from the line
            code = line[:-1]

            # Create the file path for the new file
            new_path = os.path.join(os.path.dirname(path), "crosstables", f"{code}.txt")

            # Check if the new file path exists and is not empty
            if os.path.exists(new_path) and os.path.getsize(new_path) > 0:
                # File exists and is not empty, skip this iteration
                continue

            # If the file doesn't exist or is empty, fetch data from the URL
            # Form the complete URL
            url = base_url + "view_source.phtml?code=" + code

            # Make the HTTP request
            response = requests.get(url)

            # Parse the HTML content
            soup = BeautifulSoup(response.text, "html.parser")

            # Make sure the directory exists
            os.makedirs(os.path.dirname(new_path), exist_ok=True)

            # Write the contents of 'soup' into the file
            with open(new_path, "w", encoding="utf-8") as f:
                f.write(str(soup))

        # Loop through each line in the file
        for line in lines:
            # Extract the code from the line
            code = line[:-1]

            new_path = os.path.join(os.path.dirname(path), "crosstables", f"{code}.txt")
            with open(new_path, encoding="utf-8") as fp:
                try:
                    soup = BeautifulSoup(fp, "lxml")
                except Exception as x:
                    logging.error(f"Unexpected result at path: {path}")
                    raise x
                if not soup.find(
                    string=lambda string: "Tournament report was updated or replaced, please view Tournament Details for more information."
                    in string
                ):
                    continue
            # Create the file path for the new file
            new_path = os.path.join(os.path.dirname(path), "report", f"{code}.txt")

            # Check if the new file path exists and is not empty
            if os.path.exists(new_path) and os.path.getsize(new_path) > 0:
                # File exists and is not empty, skip this iteration
                continue

            # If the file doesn't exist or is empty, fetch data from the URL
            # Form the complete URL
            url = base_url + "tournament_report.phtml?event16=" + code

            # Make the HTTP request
            response = requests.get(url)

            # Parse the HTML content
            soup = BeautifulSoup(response.text, "html.parser")

            # Make sure the directory exists
            os.makedirs(os.path.dirname(new_path), exist_ok=True)

            # Write the contents of 'soup' into the file
            with open(new_path, "w", encoding="utf-8") as f:
                f.write(str(soup))


def scrape_country_month_year(args):
    return scrape_tournament_data(*args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract FIDE tournament information from files for a specific month"
    )
    parser.add_argument(
        "--month",
        type=str,
        help="Month for processing in YYYY-MM format",
        required=True,
    )

    args = parser.parse_args()

    # Parse month/year
    year, month = map(int, args.month.split("-"))

    tasks = []

    for country in countries:
        tasks.append((country, month, year))

    # Number of processes to use
    num_processes = 6  # Adjust this as necessary

    # Using a multiprocessing Pool to run tasks concurrently
    with Pool(num_processes) as p:
        p.map(scrape_country_month_year, tasks)
