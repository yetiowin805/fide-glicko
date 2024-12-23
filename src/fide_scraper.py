import os
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
import argparse
from countries import countries
import re
import logging
from typing import Set
from datetime import date
import asyncio
from pathlib import Path

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


async def scrape_fide_data(
    session: aiohttp.ClientSession,
    country: str,
    month: int,
    year: int,
    raw_tournament_data_path: str,
    semaphore: asyncio.Semaphore,
) -> None:
    """
    Scrape FIDE tournament data for a specific country and rating period.
    """
    if not is_valid_rating_period(year, month):
        return

    month_str = f"{month:02d}"
    formatted_str = f"country={country}&rating_period={year}-{month_str}-01"
    dir_path = Path(raw_tournament_data_path) / country / f"{year}-{month_str}"

    # Check if the directory path exists
    if (dir_path / "tournaments.txt").exists():
        return

    url = f"{BASE_URL}?moder=ev_code&{formatted_str}"
    logging.info(f"Scraping data from: {url}")

    try:
        async with semaphore:
            async with session.get(url, timeout=30) as response:
                response.raise_for_status()
                text = await response.text()
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logging.error(f"Failed to fetch data for {country} {year}-{month:02d}: {e}")
        return

    soup = BeautifulSoup(text, "html.parser")
    unique_codes = {
        a["href"].split("=")[-1]
        for a in soup.find_all("a", href=True)
        if "tournament_report.phtml" in a["href"]
    }

    if unique_codes:
        dir_path.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(dir_path / "tournaments.txt", "w") as file:
            await file.write("\n".join(unique_codes) + "\n")


async def main(month: str, data_dir: str):
    # Parse month/year
    year, month_num = map(int, month.split("-"))

    # Configure async session
    max_concurrent_requests = 10
    semaphore = asyncio.Semaphore(max_concurrent_requests)

    connector = aiohttp.TCPConnector(limit=100)
    timeout = aiohttp.ClientTimeout(total=60)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [
            scrape_fide_data(
                session,
                country,
                month_num,
                year,
                os.path.join(data_dir, "raw_tournament_data"),
                semaphore,
            )
            for country in countries
        ]
        await asyncio.gather(*tasks)


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

    # Run the main asynchronous function
    asyncio.run(main(args.month, args.data_dir))
