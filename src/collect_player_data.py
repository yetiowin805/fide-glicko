import os
from pathlib import Path
from multiprocessing import Pool
from tqdm import tqdm
import itertools
import ast
import re
from datetime import datetime
import argparse
import logging
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from countries import countries

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global rating caches
fide_ratings: Dict[str, Dict] = {}
glicko_ratings: Dict[str, Dict] = {}

# Configuration constants
RATING_DEFAULT = 1500
RD_DEFAULT = 350


def setup_data_dirs(base_dir: str) -> Dict[str, Path]:
    """Set up data directories based on the provided base directory."""
    return {
        "player_info": Path(base_dir) / "player_info" / "processed",
        "rating_lists": Path(base_dir) / "rating_lists",
        "raw_tournament": Path(base_dir) / "raw_tournament_data",
        "clean_numerical": Path(base_dir) / "clean_numerical",
    }


@dataclass
class TournamentData:
    """Container for tournament-related data"""

    date: datetime
    time_control: str
    players: List[dict]


def find_latest_player_info(year: int, month: int, data_dir: Dict[str, Path]) -> Path:
    """Find the most recent player info file."""
    temp_month, temp_year = month, year
    while True:
        player_info_path = (
            data_dir["player_info"] / f"{temp_year:04d}-{temp_month:02d}.txt"
        )
        if player_info_path.exists():
            return player_info_path

        if temp_month == 1:
            temp_year -= 1
            temp_month = 12
        else:
            temp_month -= 1


def get_ratings_for_period(
    year: int, month: int, time_control: str, data_dir: Dict[str, Path]
) -> Tuple[Dict, Dict]:
    """Cache and return FIDE and Glicko ratings for a given period."""
    period_key = f"{year}-{month:02d}"

    if period_key not in fide_ratings:
        player_info_path = find_latest_player_info(year, month, data_dir)
        try:
            with open(player_info_path, "r", encoding="utf-8", errors="replace") as f:
                fide_ratings[period_key] = {
                    player["id"]: player for player in map(ast.literal_eval, f)
                }
        except Exception as e:
            logger.error(
                f"Error reading FIDE ratings from {player_info_path}: {str(e)}"
            )
            fide_ratings[period_key] = {}

    if period_key not in glicko_ratings:
        rating_path = (
            data_dir["rating_lists"] / time_control / f"{year:04d}-{month:02d}.txt"
        )
        if not rating_path.exists():
            logger.warning(f"Glicko rating file does not exist: {rating_path}")
            glicko_ratings[period_key] = {}
        else:
            try:
                with open(rating_path) as f:
                    glicko_ratings[period_key] = {
                        int(components[0]): dict(
                            zip(
                                ["id", "rating", "rd", "volatility"],
                                [int(components[0])]
                                + [float(x) for x in components[1:]],
                            )
                        )
                        for components in (
                            line.strip().split() for line in f if line.strip()
                        )
                    }
            except Exception as e:
                logger.error(
                    f"Error reading Glicko ratings from {rating_path}: {str(e)}"
                )
                glicko_ratings[period_key] = {}

    return fide_ratings.get(period_key, {}), glicko_ratings.get(period_key, {})


def parse_tournament_date(date_line: str) -> Optional[datetime]:
    """Parse tournament date with proper error handling."""
    if date_line.startswith("Date Received:"):
        date_without_prefix = date_line[15:].strip()
    else:
        date_without_prefix = date_line.strip()

    if date_without_prefix == "Not submitted":
        return None

    try:
        return datetime.strptime(date_without_prefix, "%Y-%m-%d")
    except ValueError as e:
        logger.error(f"Invalid date format: {date_without_prefix}")
        raise ValueError(f"Invalid date format: {date_without_prefix}") from e


def write_fide_data(
    source_file: str,
    destination_file: str,
    time_control: str,
    year: int,
    month: int,
    end_month: int,
    end_year: int,
    data_dir: Dict[str, Path],
) -> None:
    """Process and write FIDE tournament data."""
    try:
        with open(source_file, "r") as f:
            # Read and parse Date Received
            date_line = f.readline().strip()
            date_obj = parse_tournament_date(date_line)
            if not date_obj:
                logger.info(f"Date not submitted in file: {source_file}")
                return

            # Validate tournament date
            if date_obj.year > end_year or (
                date_obj.year == end_year and date_obj.month > end_month
            ):
                logger.info(
                    f"Tournament date {date_obj} outside range in file: {source_file}"
                )
                return

            # Read and parse Time Control
            time_control_line = f.readline().strip()
            time_control_match = re.match(r"Time Control:\s*(\w+)", time_control_line)
            if not time_control_match:
                logger.error(f"Time Control line format incorrect in {source_file}")
                return
            file_time_control = time_control_match.group(1).lower()

            if file_time_control != time_control.lower():
                return

            # Read and parse Player lines
            players = []
            for line_number, line in enumerate(f, start=3):
                if not line.strip():
                    continue  # Skip empty lines
                try:
                    player_data = ast.literal_eval(line.strip())
                    # Validate required fields
                    if not all(
                        k in player_data
                        for k in ("fide_id", "name", "number", "opponents")
                    ):
                        logger.warning(
                            f"Missing fields in player data at {source_file}:{line_number}"
                        )
                        continue
                    players.append(player_data)
                except (ValueError, SyntaxError) as e:
                    logger.error(
                        f"Error parsing player data at {source_file}:{line_number}: {str(e)}"
                    )
                    continue

        if not players:
            logger.info(f"No valid player data found in file: {source_file}")
            return

        # Build number to fide_id mapping
        number_to_fide_id = {player["number"]: player["fide_id"] for player in players}

        # Open destination file in append mode
        with open(destination_file, "a") as f2:
            for player in players:
                fide_id = player["fide_id"]
                opponents = player["opponents"]
                num_opponents = len(opponents)

                # Write player fide_id and number of opponents
                f2.write(f"{fide_id} {num_opponents}\n")

                # Write each opponent's fide_id and result
                for opponent in opponents:
                    opponent_number = opponent.get("id")
                    opponent_result = opponent.get("result", 0.0)

                    if not opponent_number:
                        logger.warning(
                            f"Missing opponent ID for player {fide_id} in file {source_file}"
                        )
                        continue

                    opponent_fide_id = number_to_fide_id.get(opponent_number)
                    if not opponent_fide_id:
                        logger.warning(
                            f"Opponent number {opponent_number} not found for player {fide_id} in file {source_file}"
                        )
                        continue

                    f2.write(f"{opponent_fide_id} {opponent_result}\n")

    except Exception as e:
        logger.error(f"Error processing {source_file}: {str(e)}")
        raise


def write_fide_data_helper(args):
    """Helper function for multiprocessing."""
    return write_fide_data(*args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process FIDE tournament data.")
    parser.add_argument(
        "--start_month", type=str, help="Start month (YYYY-MM)", required=True
    )
    parser.add_argument(
        "--end_month", type=str, help="End month (YYYY-MM)", required=True
    )
    parser.add_argument(
        "--time_control",
        type=str,
        default="standard",
        choices=["standard", "rapid", "blitz"],
        help="Time control type",
    )
    parser.add_argument(
        "--data_dir",
        type=str,
        default=".",
        help="Base directory for all data files",
    )

    args = parser.parse_args()

    try:
        start_year, start_month = map(int, args.start_month.split("-"))
        end_year, end_month = map(int, args.end_month.split("-"))
    except ValueError:
        logger.error("Invalid date format. Use YYYY-MM")
        exit(1)

    # Set up data directories
    DATA_DIR = setup_data_dirs(args.data_dir)

    # Ensure directories exist
    for directory in DATA_DIR.values():
        directory.mkdir(parents=True, exist_ok=True)

    # Process tournament files
    source_dir = DATA_DIR["raw_tournament"]
    destination_dir = DATA_DIR["clean_numerical"]
    time_control = args.time_control.lower()

    # Recursively find all processed/*.txt files
    tournament_files = list(source_dir.rglob("processed/*.txt"))
    if not tournament_files:
        logger.warning(f"No tournament files found in {source_dir}")
        exit(0)

    logger.info(f"Found {len(tournament_files)} tournament files to process.")

    tasks = []
    destination_paths_set = set()

    for file_path in tournament_files:
        try:
            # Extract formatted_date from the path: country/{formatted_date}/processed/{int}.txt
            # Assuming the structure is {source_dir}/country/YYYY-MM/processed/{integer}.txt
            formatted_date = file_path.parent.parent.name  # {formatted_date} directory
            year_str, month_str = formatted_date.split("-")
            year = int(year_str)
            month = int(month_str)

            # Check if the file's date is within the specified range
            if (year < start_year) or (year == start_year and month < start_month):
                continue
            if (year > end_year) or (year == end_year and month > end_month):
                continue

            # Define the destination path based on year, month, and time_control
            destination_path = (
                destination_dir / f"{year:04d}-{month:02d}" / time_control / "games.txt"
            )

            # Add to the set of destination paths for pre-processing
            destination_paths_set.add(destination_path)

            # Append the task
            tasks.append(
                (
                    str(file_path),
                    str(destination_path),
                    time_control,
                    year,
                    month,
                    end_month,
                    end_year,
                    DATA_DIR,
                )
            )
        except Exception as e:
            logger.error(f"Error processing file path {file_path}: {str(e)}")
            continue

    if not tasks:
        logger.warning("No tournament files found within the specified date range.")
        exit(0)

    # Prepare destination files: ensure directories exist and clear files
    for dest_path in destination_paths_set:
        try:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            # Clear the destination file
            with open(dest_path, "w") as f:
                pass
        except Exception as e:
            logger.error(f"Error preparing destination file {dest_path}: {str(e)}")
            exit(1)

    logger.info(
        f"Processing {len(tasks)} tournament files with '{time_control}' time control."
    )

    # Process files in parallel
    with Pool() as pool:
        list(
            tqdm(
                pool.imap_unordered(write_fide_data_helper, tasks),
                total=len(tasks),
                desc="Processing tournaments",
            )
        )

    logger.info("Tournament data processing completed successfully.")
