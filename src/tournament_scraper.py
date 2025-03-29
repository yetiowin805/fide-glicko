import asyncio
import aiohttp
import aiofiles
import logging
from typing import List, Optional
from pathlib import Path
from dataclasses import dataclass
import os
from bs4 import BeautifulSoup
import re
from multiprocessing import cpu_count
from countries import countries
import argparse
import json

# Constants
BASE_URL = "https://ratings.fide.com"
TOURNAMENT_REPORT_PATH = "report.phtml?event="


async def extract_player_data(soup: BeautifulSoup) -> tuple:
    """Extract player IDs and names from tournament HTML."""
    player_data = []
    time_control = None
    table = soup.find("table", {"class": "table2"})

    if table is None:
        logging.warning("Table with class 'table2' not found in tournament HTML")
        return player_data, time_control

    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if cells and len(cells) > 1:  # If there are at least 2 cells in the row
            # Extract player ID
            player_id = cells[0].text.strip()
            # Skip if player ID is not numeric
            if not player_id.isdigit():
                continue

            # Extract player name
            name_cell = cells[1]
            # Get the text content, not the HTML
            player_name = name_cell.get_text(strip=True)

            player_data.append((player_id, player_name))

            if time_control is None:
                href = cells[1].find("a")["href"]
                match = re.search("rating=(\d+)", href)
                if match:
                    time_control = match.group(1)

    return player_data, time_control


async def fetch_process_and_save(
    session: aiohttp.ClientSession,
    url: str,
    base_output_dir: Path,
    date_str: str,
    tournament_code: str,
    semaphore: asyncio.Semaphore,
) -> bool:
    """
    Fetch data from URL, process it to extract player IDs and names, and save results.

    Args:
        session: HTTP client session
        url: URL to fetch data from
        base_output_dir: Base directory for outputs
        date_str: Date string in YYYY-MM format
        tournament_code: Tournament code/ID
        semaphore: Semaphore to limit concurrent requests

    Returns:
        bool: True if successful, False otherwise
    """
    max_retries = 8
    base_delay = 1  # Initial delay in seconds

    async with semaphore:
        for attempt in range(max_retries):
            try:
                async with session.get(url, timeout=30) as response:
                    response.raise_for_status()
                    text = await response.text()
                    soup = BeautifulSoup(text, "html.parser")

                    # Extract player data and time control
                    player_data, time_control = await extract_player_data(soup)

                    if not player_data:
                        logging.warning(f"No player data found in tournament at {url}")
                        return False

                    # Map time_control value to directory name
                    time_control_dirs = {"0": "standard", "1": "rapid", "2": "blitz"}

                    # Get the time control directory (default to standard if not found)
                    tc_dir = time_control_dirs.get(time_control, "standard")

                    # Create the final save path with time control directory structure
                    final_save_path = (
                        base_output_dir / tc_dir / date_str / f"{tournament_code}.txt"
                    )

                    # Ensure the directory exists
                    final_save_path.parent.mkdir(parents=True, exist_ok=True)

                    # Save the extracted player IDs and names
                    async with aiofiles.open(
                        final_save_path, "w", encoding="utf-8"
                    ) as f:
                        for player_id, player_name in player_data:
                            await f.write(
                                json.dumps({"id": player_id, "name": player_name})
                                + "\n"
                            )

                    logging.info(
                        f"Successfully processed tournament from {url}, extracted {len(player_data)} players and saved to {final_save_path} ({tc_dir} time control)"
                    )
                    return True  # Success - exit the function

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                if attempt < max_retries - 1:  # Don't log "retrying" on last attempt
                    logging.warning(
                        f"Attempt {attempt + 1}/{max_retries} failed for {url}: {e}. Retrying in {delay}s..."
                    )
                    await asyncio.sleep(delay)
                else:
                    logging.error(
                        f"Final attempt {max_retries}/{max_retries} failed for {url}: {e}"
                    )
            except IOError as e:
                logging.error(f"Failed to save to {final_save_path}: {e}")
                return False  # Don't retry on file system errors

        return False  # Failed after all retries


async def process_tournament_data(
    session: aiohttp.ClientSession,
    country: str,
    month: int,
    year: int,
    data_dir: Path,
    semaphore: asyncio.Semaphore,
) -> None:
    """
    Scrape and process tournament data for a specific country and period.

    Args:
        session: aiohttp ClientSession for making HTTP requests
        country: Country code
        month: Month (1-12)
        year: Year
        data_dir: Base path for data
        semaphore: asyncio Semaphore to limit concurrency
    """
    month_str = f"{month:02d}"
    formatted_str = f"{year}-{month_str}"

    # Path to tournaments list
    raw_data_path = data_dir / "raw_tournament_data" / country / formatted_str
    tournament_file = raw_data_path / "tournaments.txt"

    if not tournament_file.is_file():
        logging.info(f"No tournament file found at {tournament_file}")
        return

    base_output_dir = data_dir / "processed_tournament_data"

    async with aiofiles.open(tournament_file, "r", encoding="utf-8") as f:
        tournament_codes = [line.strip() for line in await f.readlines()]

    tasks = []
    for code in tournament_codes:
        if not code:
            logging.warning(
                f"Empty tournament code found in {tournament_file}. Skipping."
            )
            continue

        url = f"{BASE_URL}/{TOURNAMENT_REPORT_PATH}{code}"
        # Pass the base output directory formatted date string, and tournament code
        # so we can construct the final path after determining time control
        tasks.append(
            fetch_process_and_save(
                session, url, base_output_dir, formatted_str, code, semaphore
            )
        )

    # Execute all fetch tasks concurrently
    if tasks:
        results = await asyncio.gather(*tasks)
        logging.info(
            f"Completed {sum(results)} of {len(tasks)} tournament downloads and processing for {country} {year}-{month:02d}"
        )


async def main(month: str, data_dir: str) -> None:
    """
    Main function to coordinate scraping and processing of tournament data.

    Args:
        month: Month in YYYY-MM format
        data_dir: Base directory for all data files
    """
    # Parse month/year
    year, month_num = map(int, month.split("-"))

    # Determine optimal number of concurrent connections
    max_concurrent_requests = 10  # Adjust based on testing and server capacity
    semaphore = asyncio.Semaphore(max_concurrent_requests)

    connector = aiohttp.TCPConnector(limit=100)  # Adjust the limit as needed
    timeout = aiohttp.ClientTimeout(total=60)  # Total timeout for a request

    data_path = Path(data_dir)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        for country in countries:
            tasks.append(
                process_tournament_data(
                    session,
                    country,
                    month_num,
                    year,
                    data_path,
                    semaphore,
                )
            )

        await asyncio.gather(*tasks)
        logging.info(f"Completed tournament data scraping and processing for {month}")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    parser = argparse.ArgumentParser(
        description="Scrape and process FIDE tournament information for a specific month"
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
