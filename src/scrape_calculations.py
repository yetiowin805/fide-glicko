import asyncio
import aiohttp
import aiofiles
import logging
from pathlib import Path
import argparse
import os
import re
from bs4 import BeautifulSoup
from datetime import datetime
import json

# Constants
BASE_URL = "https://ratings.fide.com/a_indv_calculations.php"
TIME_CONTROLS = {"standard": "0", "rapid": "1", "blitz": "2"}


def extract_calculation_data(html_content):
    """
    Extract player names, ratings, federations, and results from calculation HTML.
    Can handle multiple tournaments in a single calculation page.

    Args:
        html_content: HTML content of the calculation page

    Returns:
        dict: Structured data containing the calculation information
    """
    soup = BeautifulSoup(html_content, "html.parser")
    result_data = {
        "tournaments": [],
    }

    # First find all tournament headers
    tournament_headers = soup.find_all("div", class_="rtng_line01")
    tournament_ids = []
    tournament_names = []

    for header in tournament_headers:
        # Look for tournament links and extract IDs
        tournament_link = header.find("a")
        if tournament_link:
            href = tournament_link.get("href", "")
            tournament_id_match = re.search(r"event=(\d+)", href)
            if tournament_id_match:
                tournament_id = tournament_id_match.group(1)
                tournament_name = tournament_link.text.strip()
                tournament_ids.append(tournament_id)
                tournament_names.append(tournament_name)

    # Find all calculation tables - keeping the original method
    calc_tables = soup.find_all("table", class_="calc_table")

    # Process each table and associate with tournament ID
    for i, calc_table in enumerate(calc_tables):
        if i < len(tournament_ids):  # Make sure we have a tournament ID for this table
            tournament_id = tournament_ids[i]

            # Create tournament entry
            current_tournament = {"tournament_id": tournament_id, "games": []}

            # Check if player is unrated in this tournament by looking for the Rp header in this table
            rp_headers = calc_table.find_all("td", string=lambda s: s and "Rp" in s)
            current_tournament["player_is_unrated"] = len(rp_headers) > 0

            # Find all rows with class="list4" and bgcolor="#efefef" that contain actual player data
            game_rows = calc_table.find_all("tr", attrs={"bgcolor": "#efefef"})

            for row in game_rows:
                try:
                    cells = row.find_all("td", class_="list4")
                    if len(cells) < 6:  # Need at least name, rating, federation, result
                        continue

                    game = {}

                    # Extract opponent name - must handle the display:flex container
                    opponent_cell = cells[0]
                    game["opponent_name"] = opponent_cell.text.strip()

                    # Extract rating
                    if len(cells) > 3:
                        rating_text = cells[3].text.strip()
                        # Remove any non-digit characters except for a possible leading minus sign
                        rating = re.search(r"-?\d+", rating_text)
                        if rating:
                            game["opponent_rating"] = rating.group()

                    # Extract federation
                    if len(cells) > 4:
                        game["federation"] = cells[4].text.strip()

                    # Extract game result
                    if len(cells) > 5:
                        game["result"] = cells[5].text.strip()

                    # Add tournament_id to each game
                    game["tournament_id"] = tournament_id

                    # Add to current tournament's games
                    current_tournament["games"].append(game)

                except Exception as e:
                    logging.error(f"Error extracting game data: {e}")

            # Add tournament to results if it has games
            if current_tournament["games"]:
                result_data["tournaments"].append(current_tournament)

    return result_data


async def fetch_calculations(
    session: aiohttp.ClientSession,
    fide_id: str,
    period: str,
    time_control: str,
    output_path: Path,
    semaphore: asyncio.Semaphore,
) -> bool:
    """
    Fetch calculation data for a specific player, period, and time control.

    Args:
        session: HTTP session
        fide_id: FIDE ID of the player
        period: Rating period in YYYY-MM-01 format
        time_control: Time control code (0, 1, or 2)
        output_path: Where to save the data
        semaphore: Semaphore to limit concurrent requests

    Returns:
        bool: True if successful, False otherwise
    """
    url = f"{BASE_URL}?id_number={fide_id}&rating_period={period}&t={time_control}"
    max_retries = 5
    base_delay = 1  # Initial delay in seconds

    # Skip if output file already exists and has content
    if output_path.exists() and output_path.stat().st_size > 0:
        logging.info(
            f"Output file already exists for {fide_id} ({time_control}), skipping"
        )
        return True

    async with semaphore:
        for attempt in range(max_retries):
            try:
                async with session.get(url, timeout=30) as response:
                    response.raise_for_status()
                    html_content = await response.text()

                    # Check if there's actual calculation data
                    if (
                        "No calculations available" in html_content
                        or "No games" in html_content
                    ):
                        logging.info(
                            f"No calculation data for player {fide_id} with time control {time_control}"
                        )
                        return False

                    # Extract structured data from the HTML
                    calculation_data = extract_calculation_data(html_content)

                    # Ensure output directory exists
                    output_path.parent.mkdir(parents=True, exist_ok=True)

                    # Save the extracted data as JSON
                    async with aiofiles.open(output_path, "w", encoding="utf-8") as f:
                        await f.write(json.dumps(calculation_data, indent=2))

                    return True

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                if attempt < max_retries - 1:
                    logging.warning(
                        f"Attempt {attempt + 1}/{max_retries} failed for {fide_id}: {e}. Retrying in {delay}s..."
                    )
                    await asyncio.sleep(delay)
                else:
                    logging.error(f"Failed to fetch data for player {fide_id}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error processing {fide_id}: {e}")
                return False

        return False  # All retries failed


async def process_player_list(
    session: aiohttp.ClientSession,
    player_file: Path,
    period: str,
    data_dir: Path,
    time_control: str,
    semaphore: asyncio.Semaphore,
) -> None:
    """
    Process a list of player IDs for a specific time control.

    Args:
        session: HTTP session
        player_file: File containing player IDs
        period: Rating period (YYYY-MM)
        data_dir: Base data directory
        time_control: Time control name (standard, rapid, blitz)
        semaphore: Semaphore to limit concurrent requests
    """
    try:
        # Read player IDs from file
        async with aiofiles.open(player_file, "r", encoding="utf-8") as f:
            content = await f.read()
            player_ids = [line.strip() for line in content.split("\n") if line.strip()]

        # Create output directory for this period and time control
        output_dir = data_dir / "calculations" / period / time_control
        output_dir.mkdir(parents=True, exist_ok=True)

        # Setup tasks
        tasks = []
        tc_code = TIME_CONTROLS[time_control]
        period_formatted = f"{period}-01"  # Add day for API format

        for player_id in player_ids:
            output_path = output_dir / f"{player_id}.json"
            tasks.append(
                fetch_calculations(
                    session,
                    player_id,
                    period_formatted,
                    tc_code,
                    output_path,
                    semaphore,
                )
            )

        # Process in batches to avoid memory issues with very large lists
        batch_size = 500
        total_batches = (len(tasks) + batch_size - 1) // batch_size

        for i in range(0, len(tasks), batch_size):
            batch = tasks[i : i + batch_size]
            batch_num = i // batch_size + 1
            logging.info(
                f"Processing batch {batch_num}/{total_batches} for {time_control} time control"
            )

            results = await asyncio.gather(*batch)
            success_count = sum(1 for result in results if result)

            logging.info(
                f"Completed batch {batch_num}: {success_count}/{len(batch)} successful"
            )

    except Exception as e:
        logging.error(f"Error processing player list {player_file}: {e}")


async def main(month: str, data_dir: str) -> None:
    """
    Main function to coordinate scraping of player calculation data.

    Args:
        month: Month in YYYY-MM format
        data_dir: Base directory for all data files
    """
    # Parse month
    try:
        year, month_num = map(int, month.split("-"))
        period = month  # YYYY-MM format
    except ValueError:
        logging.error(f"Invalid month format: {month}. Expected YYYY-MM")
        return

    data_path = Path(data_dir)
    active_players_dir = data_path / "active_players"

    # Determine which time controls to process based on year
    time_controls_to_process = ["standard"]
    if year >= 2012:
        time_controls_to_process.extend(["rapid", "blitz"])

    # Set up HTTP session with rate limiting
    max_concurrent_requests = 20
    semaphore = asyncio.Semaphore(max_concurrent_requests)

    connector = aiohttp.TCPConnector(limit=100)
    timeout = aiohttp.ClientTimeout(total=60)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []

        # Process each time control
        for tc in time_controls_to_process:
            player_file = active_players_dir / f"{period}_{tc}.txt"

            if not player_file.exists():
                logging.warning(
                    f"No player file found for {tc} time control: {player_file}"
                )
                continue

            logging.info(
                f"Processing player list for {tc} time control from {player_file}"
            )

            tasks.append(
                process_player_list(
                    session, player_file, period, data_path, tc, semaphore
                )
            )

        # Run all time control processing tasks
        await asyncio.gather(*tasks)

    logging.info(f"Completed scraping calculation data for {month}")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    parser = argparse.ArgumentParser(
        description="Scrape FIDE rating calculations for active players"
    )
    parser.add_argument(
        "--month",
        type=str,
        help="Month for processing in YYYY-MM format",
        required=True,
    )
    parser.add_argument(
        "--data_dir", type=str, help="Base directory for all data files", default="."
    )

    args = parser.parse_args()

    # Run the main asynchronous function
    asyncio.run(main(args.month, args.data_dir))
