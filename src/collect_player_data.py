import re
import os
import ast
from multiprocessing import Pool, Lock, Manager
from tqdm import tqdm
import itertools
import numpy as np
from datetime import datetime
import argparse
from countries import countries
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# Configure logging
logging.basicConfig(
    filename="collect_player_data.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global rating caches
fide_ratings: Dict[str, Dict] = {}
glicko_ratings: Dict[str, Dict] = {}

# Configuration constants
RATING_DEFAULT = 1500
RD_DEFAULT = 350

lock: Optional[Lock] = None


def setup_data_dirs(base_dir: str) -> Dict[str, Path]:
    """Set up data directories based on the provided base directory."""
    return {
        "player_info": Path(base_dir) / "player_info" / "processed",
        "rating_lists": Path(base_dir) / "rating_lists",
        "raw_tournament": Path(base_dir) / "raw_tournament_data",
        "clean_numerical": Path(base_dir) / "clean_numerical",
    }


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

        # Prevent infinite loop
        if temp_year < 2000:
            logger.error("No available player_info files found.")
            return Path()


def get_ratings_for_period(
    year: int, month: int, time_control: str, data_dir: Dict[str, Path]
) -> Tuple[Dict, Dict]:
    """Cache and return FIDE and Glicko ratings for a given period."""
    period_key = f"{year}-{month:02d}"
    if period_key not in fide_ratings:
        player_info_path = find_latest_player_info(year, month, data_dir)
        logger.info(f"Player info path: {player_info_path}")
        if not player_info_path.exists():
            fide_ratings[period_key] = {}
        else:
            try:
                with open(
                    player_info_path, "r", encoding="utf-8", errors="replace"
                ) as f:
                    fide_ratings[period_key] = {
                        player["id"]: player
                        for player in map(ast.literal_eval, f)
                        if "id" in player
                    }
                logger.info(f"Loaded FIDE ratings for {period_key}")
            except Exception as e:
                logger.error(f"Error reading FIDE ratings from {player_info_path}: {e}")
                fide_ratings[period_key] = {}

    if period_key not in glicko_ratings:
        rating_path = (
            data_dir["rating_lists"] / time_control / f"{year:04d}-{month:02d}.txt"
        )
        logger.info(f"Glicko rating path: {rating_path}")
        if not rating_path.exists():
            logger.warning(f"Glicko rating file does not exist: {rating_path}")
            glicko_ratings[period_key] = {}
        else:
            try:
                logger.info(f"Reading Glicko rating file: {rating_path}")
                with open(rating_path, "r", encoding="utf-8") as f:
                    glicko_ratings[period_key] = {
                        int(components[0]): {
                            "id": int(components[0]),
                            "rating": float(components[1]),
                            "rd": float(components[2]),
                            "volatility": float(components[3]),
                        }
                        for components in (
                            line.strip().split() for line in f if line.strip()
                        )
                        if len(components) >= 4
                    }
                logger.info(f"Loaded Glicko ratings for {period_key}")
            except Exception as e:
                logger.error(f"Error reading Glicko ratings from {rating_path}: {e}")
                glicko_ratings[period_key] = {}

    return fide_ratings.get(period_key, {}), glicko_ratings.get(period_key, {})


def get_target_date(year: int, month: int) -> Tuple[int, int]:
    """
    Determine the target year and month based on input year and month.

    Args:
        year (int): The current year.
        month (int): The current month (1-12).

    Returns:
        Tuple[int, int]: A tuple containing the target year and target month.
    """
    if not 1 <= month <= 12:
        raise ValueError("Month must be between 1 and 12.")

    if year < 2009 or (year == 2009 and month <= 6):
        target_month = ((month - 1) // 3) * 3 + 4
    elif year < 2012 or (year == 2012 and month <= 6):
        target_month = ((month - 1) // 2) * 2 + 3
    else:
        target_month = month + 1 if month < 12 else 1

    if target_month == 1:
        year += 1
    elif target_month > 12:
        target_month -= 12
        year += 1

    return year, target_month


def is_valid_tournament_date(
    target_year: int, target_month: int, tournament_year: int, tournament_month: int
) -> bool:
    """
    Validate the tournament date based on the target date's group and position.

    Args:
        target_year (int): The target year.
        target_month (int): The target month.
        tournament_year (int): The tournament's year.
        tournament_month (int): The tournament's month.

    Returns:
        bool: True if the tournament date is valid, False otherwise.
    """
    if target_year < 2009 or (target_year == 2009 and target_month < 7):
        if tournament_year == 0 and tournament_month == 0:
            return target_month % 3 == 0
        if target_month % 3 == 1:
            return tournament_year < target_year or (
                tournament_year == target_year and tournament_month < target_month
            )
        elif target_month % 3 == 2:
            return tournament_year == target_year and tournament_month == target_month
        return tournament_year > target_year or (
            tournament_year == target_year and tournament_month > target_month
        )
    elif target_year < 2012 or (target_year == 2012 and target_month < 7):
        if tournament_year == 0 and tournament_month == 0:
            return target_month % 2 == 0
        if target_month % 2 == 1:
            return tournament_year < target_year or (
                tournament_year == target_year and tournament_month < target_month
            )
        return tournament_year > target_year or (
            tournament_year == target_year and tournament_month > target_month
        )
    return True


def parse_tournament_date(date_line: str) -> Tuple[Optional[datetime], bool]:
    """Parse tournament date with proper error handling and determine crosstable type."""
    if date_line.startswith("Date Received:"):
        date_without_prefix = date_line[15:].strip()
    else:
        date_without_prefix = date_line.strip()

    if date_without_prefix == "Not submitted":
        return None, False

    try:
        date_object = datetime.strptime(date_without_prefix, "%Y-%m-%d")
        return date_object, True
    except ValueError as e:
        logger.error(f"Invalid date format: {date_without_prefix}")
        return None, False


def write_fide_data(
    source_file: str,
    time_control: str,
    month: int,
    year: int,
    data_dir: Dict[str, Path],
) -> None:
    """Process and write FIDE tournament data."""
    global lock
    try:
        with open(source_file, "r", encoding="utf-8") as f:
            # Read and parse Date Received
            date_line = f.readline().strip()
            date_obj, has_valid_date = parse_tournament_date(date_line)
            if not has_valid_date:
                logger.info(f"Date not submitted or invalid in file: {source_file}")
                return

            # Validate tournament date based on group and position
            if not is_valid_tournament_date(year, month, date_obj.year, date_obj.month):
                logger.info(
                    f"Tournament date {date_obj.strftime('%Y-%m-%d')} is not valid for target date {year}-{month:02d} in file {source_file}"
                )
                return

            # Read and parse Time Control
            time_control_line = f.readline().strip()
            time_control_match = re.match(r"Time Control:\s*(\w+)", time_control_line)
            if not time_control_match:
                logger.error(f"Time Control line format incorrect in {source_file}")
                return
            file_time_control = time_control_match.group(1)

            if file_time_control != time_control:
                logger.info(
                    f"Time control mismatch in file {source_file}: Expected '{time_control}', Found '{file_time_control}'"
                )
                return

            # Read the next line to determine crosstable type
            first_player_line = f.readline().strip()
            if not first_player_line:
                logger.warning(f"No player data found in file: {source_file}")
                return

            try:
                first_player = ast.literal_eval(first_player_line)
            except (ValueError, SyntaxError) as e:
                logger.error(f"Error parsing player data in file {source_file}: {e}")
                return

            destination_file = (
                data_dir["clean_numerical"]
                / f"{year:04d}-{month:02d}"
                / time_control
                / "games.txt"
            )

            # Ensure the destination directory exists
            destination_file.parent.mkdir(parents=True, exist_ok=True)

            # Determine crosstable type based on presence of 'opponents' key
            if "opponents" in first_player:
                # Regular Crosstable
                players = [first_player]
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
                            f"Error parsing player data at {source_file}:{line_number}: {e}"
                        )
                        continue

                if not players:
                    logger.info(f"No valid player data found in file: {source_file}")
                    return

                # Build number to fide_id mapping
                number_to_fide_id = {
                    player["number"]: player["fide_id"] for player in players
                }

                # Acquire lock before writing
                with lock:
                    # Open destination file in append mode
                    with open(destination_file, "a", encoding="utf-8") as f2:
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

                                opponent_fide_id = number_to_fide_id.get(
                                    opponent_number
                                )
                                if not opponent_fide_id:
                                    logger.warning(
                                        f"Opponent number {opponent_number} not found for player {fide_id} in file {source_file}"
                                    )
                                    continue

                                f2.write(f"{opponent_fide_id} {opponent_result}\n")

            elif all(key in first_player for key in ("RC", "score", "N")):
                # Missing Crosstable
                players = [first_player]
                for line_number, line in enumerate(f, start=3):
                    if not line.strip():
                        continue  # Skip empty lines
                    try:
                        player_data = ast.literal_eval(line.strip())
                        # Validate required fields
                        if not all(
                            k in player_data for k in ("fide_id", "RC", "score", "N")
                        ):
                            logger.warning(
                                f"Missing fields in player data at {source_file}:{line_number}"
                            )
                            continue
                        players.append(player_data)
                    except (ValueError, SyntaxError) as e:
                        logger.error(
                            f"Error parsing player data at {source_file}:{line_number}: {e}"
                        )
                        continue

                if not players:
                    logger.info(f"No valid player data found in file: {source_file}")
                    return

                # Load FIDE and Glicko ratings
                period_key = f"{year}-{month:02d}"
                current_fide_ratings, current_glicko_ratings = get_ratings_for_period(
                    year, month, time_control, data_dir
                )

                fide_ids = [player["fide_id"] for player in players]

                tournament_fide_ratings = [
                    int(
                        current_fide_ratings.get(fide_id, {}).get(
                            "rating", RATING_DEFAULT
                        )
                    )
                    for fide_id in fide_ids
                    if fide_id in current_fide_ratings
                ]
                tournament_glicko_ratings = [
                    current_glicko_ratings.get(int(fide_id), {}).get(
                        "rating", RATING_DEFAULT
                    )
                    for fide_id in fide_ids
                    if fide_id in current_fide_ratings
                ]
                tournament_rds = [
                    current_glicko_ratings.get(int(fide_id), {}).get("rd", RD_DEFAULT)
                    for fide_id in fide_ids
                    if fide_id in current_fide_ratings
                ]

                if tournament_fide_ratings:
                    m, b = np.polyfit(
                        tournament_fide_ratings, tournament_glicko_ratings, 1
                    )
                    average_rd = (
                        sum(tournament_rds) / len(tournament_rds)
                        if tournament_rds
                        else RD_DEFAULT
                    )
                else:
                    m, b = 0, RATING_DEFAULT
                    average_rd = RD_DEFAULT

                # Acquire lock before writing
                with lock:
                    # Open destination file in append mode
                    with open(destination_file, "a", encoding="utf-8") as f2:
                        for player in players:
                            fide_id = player["fide_id"]
                            try:
                                rc = float(player["RC"])
                                score = float(player["score"])
                                N = int(player["N"])
                            except (ValueError, TypeError) as e:
                                logger.warning(
                                    f"Invalid RC, score, or N for player {fide_id} in file {source_file}: {e}"
                                )
                                continue

                            average_glicko = m * rc + b
                            average_score = score / N if N != 0 else 0.0

                            f2.write(f"{fide_id} {N}\n")
                            for _ in range(N):
                                f2.write(
                                    f"{average_glicko} {average_rd} {average_score}\n"
                                )

            else:
                logger.error(
                    f"Unknown crosstable format in file {source_file}: Missing both 'opponents' and 'RC' fields."
                )
                return

    except Exception as e:
        logger.error(f"Error processing {source_file}: {e}")
        return


def write_fide_data_helper(args):
    """Helper function for multiprocessing."""
    return write_fide_data(*args)


def initializer_process(l: Lock):
    """Initializer for Pool workers to set the global lock."""
    global lock
    lock = l


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Download FIDE player information.")
    parser.add_argument(
        "--month",
        type=str,
        help="Month for the download in YYYY-MM format",
        required=True,
    )
    parser.add_argument(
        "--time_control",
        type=str,
        default="Standard",
        choices=["Standard", "Rapid", "Blitz"],
        help="Time control type",
        required=True,
    )
    parser.add_argument(
        "--data_dir",
        type=str,
        default=".",
        help="Base directory for all data files",
        required=True,
    )

    # Parse arguments
    args = parser.parse_args()

    # Validate month format
    def validate_month_format(month_str: str) -> bool:
        try:
            datetime.strptime(month_str, "%Y-%m")
            return True
        except ValueError:
            return False

    if not validate_month_format(args.month):
        logger.error("Invalid month format. Use YYYY-MM")
        exit(1)

    # Parse month/year
    year, month = map(int, args.month.split("-"))

    # Determine target year and month based on input
    target_year, target_month = get_target_date(year, month)

    # Set up data directories
    DATA_DIR = setup_data_dirs(args.data_dir)

    # Ensure directories exist
    for directory in DATA_DIR.values():
        directory.mkdir(parents=True, exist_ok=True)

    # Process tournament files
    source_dir = DATA_DIR["raw_tournament"]
    destination_dir = DATA_DIR["clean_numerical"]
    time_control = args.time_control

    # Collect tournament files from each federation's target folder
    tournament_files = []
    destination_paths_set = set()  # To collect unique destination paths

    for fed in countries:
        fed_tournament_dir = source_dir / fed / f"{target_year:04d}-{target_month:02d}"
        if not fed_tournament_dir.exists():
            logger.warning(
                f"No tournament files found for federation {fed} in {fed_tournament_dir}"
            )
            continue
        fed_files = list(fed_tournament_dir.glob("processed/*.txt"))
        if not fed_files:
            logger.warning(f"No .txt files found in {fed_tournament_dir}")
        tournament_files.extend(fed_files)

    if not tournament_files:
        logger.warning("No tournament files found in the target directories.")
        exit(0)
    logger.info(f"Found {len(tournament_files)} tournament files to process.")

    tasks = []

    for file_path in tournament_files:
        try:
            # Define the destination path based on target year, month, and time_control
            destination_path = (
                destination_dir
                / f"{target_year:04d}-{target_month:02d}"
                / time_control
                / "games.txt"
            )
            destination_paths_set.add(destination_path)

            tasks.append(
                (
                    str(file_path),
                    time_control,
                    month,
                    year,
                    DATA_DIR,
                )
            )
        except Exception as e:
            logger.error(f"Error processing file path {file_path}: {e}")
            continue

    if not tasks:
        logger.warning("No tournament files found within the specified date range.")
        exit(0)

    # Prepare destination files: ensure directories exist and clear files
    for dest_path in destination_paths_set:
        try:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            # Clear the destination file
            with open(dest_path, "w", encoding="utf-8") as f:
                pass
        except Exception as e:
            logger.error(f"Error preparing destination file {dest_path}: {e}")
            exit(1)

    logger.info(
        f"Processing {len(tasks)} tournament files with '{time_control}' time control for {args.month}."
    )

    # Create a Manager to share the Lock
    manager = Manager()
    shared_lock = manager.Lock()

    # Using a multiprocessing Pool to run tasks concurrently with the lock initializer
    with Pool(initializer=initializer_process, initargs=(shared_lock,)) as pool:
        list(
            tqdm(
                pool.imap_unordered(write_fide_data_helper, tasks),
                total=len(tasks),
                desc="Processing files",
            )
        )

    logger.info("FIDE player data collection completed successfully.")
