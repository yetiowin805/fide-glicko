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
BASE_URL = "https://ratings.fide.com/a_tournamnets.php"
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
        return (month < 7 and month % 2 == 1) or month >= 7
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

    # Create directory if it doesn't exist
    os.makedirs(dir_path, exist_ok=True)

    api_url = "https://ratings.fide.com/a_tournaments.php"
    params = {"country": country, "period": f"{year}-{month_str}-01"}

    async with semaphore:
        async with session.get(api_url, params=params, timeout=30) as api_response:
            api_response.raise_for_status()
            content = await api_response.text()

    # Parse HTML with BeautifulSoup
    soup = BeautifulSoup(content, "html.parser")

    content = content.replace("</a>", "")
    content = content.replace("&lt;", "<")
    content = content.replace("&gt;", ">")

    import json

    data = json.loads(content)

    tournament_ids = []

    # Extract tournament IDs from the data
    if "data" in data and isinstance(data["data"], list):
        for tournament in data["data"]:
            if isinstance(tournament, list) and len(tournament) > 0:
                tournament_ids.append(tournament[0])

    if tournament_ids:
        async with aiofiles.open(dir_path / "tournaments.txt", "w") as f:
            await f.write("\n".join(tournament_ids))
        logging.info(
            f"Saved {len(tournament_ids)} tournaments for {country} {year}-{month_str}"
        )
    else:
        logging.info(f"No tournaments found for {country} {year}-{month_str}")


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
