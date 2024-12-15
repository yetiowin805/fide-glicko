import logging
from typing import List
from pathlib import Path
from dataclasses import dataclass
import os
import requests
from bs4 import BeautifulSoup
import re
from multiprocessing import Pool
import argparse
from countries import countries

# Constants
BASE_URL = "https://ratings.fide.com"
TOURNAMENT_DETAILS_PATH = "tournament_details.phtml?event="
TOURNAMENT_SOURCE_PATH = "view_source.phtml?code="
TOURNAMENT_REPORT_PATH = "tournament_report.phtml?event16="


@dataclass
class TournamentPaths:
    """Container for tournament-related file paths"""

    info: Path
    crosstable: Path
    report: Path


def get_tournament_paths(base_path: Path, code: str) -> TournamentPaths:
    """Generate all required paths for a tournament"""
    return TournamentPaths(
        info=base_path / "info" / f"{code}.txt",
        crosstable=base_path / "crosstables" / f"{code}.txt",
        report=base_path / "report" / f"{code}.txt",
    )


def fetch_and_save(url: str, save_path: Path) -> None:
    """Fetch data from URL and save to file with error handling"""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        save_path.parent.mkdir(parents=True, exist_ok=True)
        save_path.write_text(str(soup), encoding="utf-8")
    except requests.RequestException as e:
        logging.error(f"Failed to fetch {url}: {e}")
    except IOError as e:
        logging.error(f"Failed to save to {save_path}: {e}")


def scrape_tournament_data(
    country: str, month: int, year: int, raw_tournament_data_path: str
) -> None:
    """
    Scrape detailed tournament data for a specific country and period.

    Args:
        country: Country code
        month: Month (1-12)
        year: Year
    """
    base_path = Path(raw_tournament_data_path) / country / f"{year}-{month:02d}"
    tournament_file = base_path / "tournaments.txt"

    if not tournament_file.is_file():
        logging.info(f"No tournament file found at {tournament_file}")
        return

    tournament_codes = tournament_file.read_text().splitlines()

    for code in tournament_codes:
        paths = get_tournament_paths(base_path, code.strip())

        # Fetch tournament details if needed
        if not paths.info.exists() or paths.info.stat().st_size == 0:
            fetch_and_save(f"{BASE_URL}/{TOURNAMENT_DETAILS_PATH}{code}", paths.info)

        # Fetch crosstable if needed
        if not paths.crosstable.exists() or paths.crosstable.stat().st_size == 0:
            fetch_and_save(
                f"{BASE_URL}/{TOURNAMENT_SOURCE_PATH}{code}", paths.crosstable
            )

        # Check if we need to fetch the report
        if paths.crosstable.exists():
            try:
                soup = BeautifulSoup(
                    paths.crosstable.read_text(encoding="utf-8"), "lxml"
                )
                needs_report = soup.find(
                    string=lambda s: "Tournament report was updated or replaced"
                    in str(s)
                )

                if needs_report and (
                    not paths.report.exists() or paths.report.stat().st_size == 0
                ):
                    fetch_and_save(
                        f"{BASE_URL}/{TOURNAMENT_REPORT_PATH}{code}", paths.report
                    )
            except Exception as e:
                logging.error(f"Error processing crosstable for {code}: {e}")


def scrape_country_month_year(args: tuple) -> None:
    """Wrapper function for multiprocessing"""
    try:
        scrape_tournament_data(*args)
    except Exception as e:
        logging.error(f"Error processing {args}: {e}")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    parser = argparse.ArgumentParser(
        description="Extract FIDE tournament information from files for a specific month"
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

    tasks = []

    for country in countries:
        tasks.append((country, month, year, os.path.join(args.data_dir, "raw_tournament_data")))

    # Number of processes to use
    num_processes = 6  # Adjust this as necessary

    # Using a multiprocessing Pool to run tasks concurrently
    with Pool(num_processes) as p:
        p.map(scrape_country_month_year, tasks)
