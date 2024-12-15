import os
import requests
from bs4 import BeautifulSoup
import argparse
from countries import countries
import re
import logging
from typing import Set
from datetime import date

# Add constants at the top
BASE_URL = "https://ratings.fide.com/tournament_list.phtml"
TOURNAMENT_REPORT_URL = "tournament_report.phtml"


def is_valid_rating_period(year: int, month: int) -> bool:
    """
    Validate if the given year and month combination is a valid FIDE rating period.
    """
    if year < 2009:
        return month % 3 == 1
    if year == 2009:
        return month < 7 and month % 3 == 1 or month >= 7 and month % 2 == 1
    if year < 2012:
        return month % 2 == 1
    if year == 2012:
        return month < 7 and month % 2 == 1
    return True


def scrape_fide_data(
    country: str, month: int, year: int, raw_tournament_data_path: str
) -> None:
    """
    Scrape FIDE tournament data for a specific country and rating period.

    Args:
        country: Country code
        month: Month (1-12)
        year: Year (>=2001)
        raw_tournament_data_path: Base path for storing tournament data
    """
    if not is_valid_rating_period(year, month):
        return

    # Pad the month with a leading zero if it's less than 10
    month_str = f"{month:02d}"
    # Create the formatted string
    formatted_str = f"country={country}&rating_period={year}-{month_str}-01"

    dir_path = os.path.join(raw_tournament_data_path, country, f"{year}-{month_str}")

    # Check if the directory path exists
    if os.path.exists(os.path.join(dir_path, "tournaments.txt")):
        return

    # Generate the URL for the specific month and year
    url = f"{BASE_URL}?moder=ev_code&{formatted_str}"

    logging.info(f"Scraping data from: {url}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch data for {country} {year}-{month:02d}: {e}")
        return

    # Parse the HTML content
    soup = BeautifulSoup(response.text, "html.parser")

    # Use a set to store unique hrefs
    unique_codes = set()

    # Find all <a> elements
    a_elements = soup.find_all("a", href=True)

    # Filter and add unique hrefs to the set
    for a in a_elements:
        if "tournament_report.phtml" in a["href"]:
            unique_codes.add(a["href"].split("=")[-1])

    if unique_codes:
        # Create the directory path
        os.makedirs(dir_path, exist_ok=True)

        with open(os.path.join(dir_path, "tournaments.txt"), "w") as file:
            for element in unique_codes:
                file.write(str(element) + "\n")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    parser = argparse.ArgumentParser(
        description="Get FIDE tournaments from a specific month."
    )
    parser.add_argument(
        "--month",
        type=str,
        help="Month for processing in YYYY-MM format",
        required=True,
    )
    parser.add_argument(
        "--data_dir",
        type=str,
        help="Base directory for all data files",
        default=".",
    )

    args = parser.parse_args()

    # Parse month/year
    year, month = map(int, args.month.split("-"))

    for country in countries:
        scrape_fide_data(
            country, month, year, os.path.join(args.data_dir, "raw_tournament_data")
        )
