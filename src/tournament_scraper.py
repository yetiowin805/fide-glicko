import asyncio
import aiohttp
import aiofiles
import logging
from typing import List
from pathlib import Path
from dataclasses import dataclass
import os
from bs4 import BeautifulSoup
import re
from multiprocessing import cpu_count
from countries import countries  # Ensure this is an async-compatible import if necessary
import argparse

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

async def get_tournament_paths(base_path: Path, code: str) -> TournamentPaths:
    """Generate all required paths for a tournament"""
    return TournamentPaths(
        info=base_path / "info" / f"{code}.txt",
        crosstable=base_path / "crosstables" / f"{code}.txt",
        report=base_path / "report" / f"{code}.txt",
    )

async def fetch_and_save(session: aiohttp.ClientSession, url: str, save_path: Path, semaphore: asyncio.Semaphore) -> None:
    """Fetch data from URL and save to file with error handling and retries"""
    max_retries = 8
    base_delay = 1  # Initial delay in seconds
    
    async with semaphore:
        for attempt in range(max_retries):
            try:
                async with session.get(url, timeout=30) as response:
                    response.raise_for_status()
                    text = await response.text()
                    soup = BeautifulSoup(text, "html.parser")

                    save_path.parent.mkdir(parents=True, exist_ok=True)
                    async with aiofiles.open(save_path, 'w', encoding='utf-8') as f:
                        await f.write(str(soup))
                    logging.info(f"Successfully fetched and saved {url} to {save_path}")
                    return  # Success - exit the function
                    
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                if attempt < max_retries - 1:  # Don't log "retrying" on last attempt
                    logging.warning(f"Attempt {attempt + 1}/{max_retries} failed for {url}: {e}. Retrying in {delay}s...")
                    await asyncio.sleep(delay)
                else:
                    logging.error(f"Final attempt {max_retries}/{max_retries} failed for {url}: {e}")
            except IOError as e:
                logging.error(f"Failed to save to {save_path}: {e}")
                return  # Don't retry on file system errors

async def scrape_tournament_data(
    session: aiohttp.ClientSession,
    country: str,
    month: int,
    year: int,
    raw_tournament_data_path: str,
    semaphore: asyncio.Semaphore
) -> None:
    """
    Scrape detailed tournament data for a specific country and period.

    Args:
        country: Country code
        month: Month (1-12)
        year: Year
        session: aiohttp ClientSession for making HTTP requests
        semaphore: asyncio Semaphore to limit concurrency
    """
    base_path = Path(raw_tournament_data_path) / country / f"{year}-{month:02d}"
    tournament_file = base_path / "tournaments.txt"

    if not tournament_file.is_file():
        logging.info(f"No tournament file found at {tournament_file}")
        return

    async with aiofiles.open(tournament_file, 'r', encoding='utf-8') as f:
        tournament_codes = [line.strip() for line in await f.readlines()]

    tasks = []
    for code in tournament_codes:
        paths = await get_tournament_paths(base_path, code)

        # Fetch tournament details if needed
        if not paths.info.exists() or paths.info.stat().st_size == 0:
            url = f"{BASE_URL}/{TOURNAMENT_DETAILS_PATH}{code}"
            tasks.append(fetch_and_save(session, url, paths.info, semaphore))
        else:
            logging.info(f"Tournament info file already exists for {code}, skipping download")

        # Fetch crosstable if needed
        if not paths.crosstable.exists() or paths.crosstable.stat().st_size == 0:
            url = f"{BASE_URL}/{TOURNAMENT_SOURCE_PATH}{code}"
            tasks.append(fetch_and_save(session, url, paths.crosstable, semaphore))
        else:
            logging.info(f"Tournament crosstable file already exists for {code}, skipping download")

        # Check if we need to fetch the report
        if paths.crosstable.exists():
            try:
                async with aiofiles.open(paths.crosstable, 'r', encoding='utf-8') as f_ct:
                    crosstable_content = await f_ct.read()
                    soup = BeautifulSoup(crosstable_content, "lxml")
                    needs_report = soup.find(
                        string=lambda s: "Tournament report was updated or replaced" in str(s)
                    )

                    if needs_report and (not paths.report.exists() or paths.report.stat().st_size == 0):
                        url = f"{BASE_URL}/{TOURNAMENT_REPORT_PATH}{code}"
                        tasks.append(fetch_and_save(session, url, paths.report, semaphore))
            except Exception as e:
                logging.error(f"Error processing crosstable for {code}: {e}")

    # Execute all fetch tasks concurrently
    await asyncio.gather(*tasks)

async def main(month: str, data_dir: str):
    # Parse month/year
    year, month_num = map(int, month.split("-"))

    # Determine optimal number of concurrent connections
    max_concurrent_requests = 10  # Adjust based on testing and server capacity
    semaphore = asyncio.Semaphore(max_concurrent_requests)

    connector = aiohttp.TCPConnector(limit=100)  # Adjust the limit as needed
    timeout = aiohttp.ClientTimeout(total=60)  # Total timeout for a request

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        for country in countries:
            tasks.append(
                scrape_tournament_data(
                    session,
                    country,
                    month_num,
                    year,
                    os.path.join(data_dir, "raw_tournament_data"),
                    semaphore
                )
            )

        await asyncio.gather(*tasks)

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

    # Run the main asynchronous function
    asyncio.run(main(args.month, args.data_dir))
