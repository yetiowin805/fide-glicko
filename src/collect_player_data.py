import re
import os
from pathlib import Path
import ast
from multiprocessing import Pool
from tqdm import tqdm
import itertools
import numpy as np
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
        "player_info": Path(base_dir) / "player_info/processed",
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
    period_key = f"{year}-{month}"

    if period_key not in fide_ratings:
        player_info_path = find_latest_player_info(year, month, data_dir)
        with open(player_info_path, "r", encoding="utf-8", errors="replace") as f:
            fide_ratings[period_key] = {player["id"]: player for player in map(eval, f)}

    if period_key not in glicko_ratings:
        rating_path = (
            data_dir["rating_lists"] / time_control / f"{year:04d}-{month:02d}.txt"
        )
        with open(rating_path) as f:
            glicko_ratings[period_key] = {
                int(components[0]): dict(
                    zip(
                        ["id", "rating", "rd", "volatility"],
                        [int(components[0])] + [float(x) for x in components[1:]],
                    )
                )
                for components in (line.split() for line in f)
            }

    return fide_ratings[period_key], glicko_ratings[period_key]


def parse_tournament_date(date_line: str) -> Optional[datetime]:
    """Parse tournament date with proper error handling."""
    date_without_prefix = (
        date_line[15:].strip()
        if date_line.startswith("Date Received:")
        else date_line.strip()
    )

    if date_without_prefix == "Not submitted":
        return None

    try:
        return datetime.strptime(date_without_prefix, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format: {date_without_prefix}") from e


def calculate_tournament_averages(
    ratings_data: dict, raw_players: List[dict]
) -> Tuple[float, float, float]:
    """Calculate tournament rating averages and transformation."""
    tournament_fide_ratings = [r["fide"] for r in ratings_data.values()]
    tournament_glicko_ratings = [r["glicko"] for r in ratings_data.values()]
    tournament_rds = [r["rd"] for r in ratings_data.values()]

    if not tournament_fide_ratings:
        return 0, RATING_DEFAULT, RD_DEFAULT

    m, b = np.polyfit(tournament_fide_ratings, tournament_glicko_ratings, 1)
    average_rd = sum(tournament_rds) / len(tournament_rds)

    return m, b, average_rd


def write_fide_data(
    source_file: str,
    destination_file: str,
    time_control: str,
    month: int,
    year: int,
    end_month: int,
    end_year: int,
    data_dir: Dict[str, Path],
) -> None:
    """Process and write FIDE tournament data."""
    try:
        with open(source_file, "r") as f:
            first_line = f.readline().strip()
            has_crosstable = "No Crosstable" not in first_line

            # Parse date
            date_line = f.readline().strip() if has_crosstable else first_line
            date_obj = parse_tournament_date(date_line)
            if not date_obj:
                return

            # Validate tournament date
            if date_obj.year > end_year or (
                date_obj.year == end_year and date_obj.month > end_month
            ):
                return

            # Validate time control
            file_time_control = f.readline().split(":")[1].strip()
            if file_time_control != time_control:
                return

            # Process based on crosstable availability
            if has_crosstable:
                process_with_crosstable(f, destination_file)
            else:
                process_no_crosstable(
                    f, destination_file, year, month, time_control, data_dir
                )

    except Exception as e:
        logger.error(f"Error processing {source_file}: {str(e)}")
        raise


def process_with_crosstable(f, destination_file: str) -> None:
    """Process tournament data with crosstable."""
    players = [eval(line.strip()) for line in f]
    fide_id_to_player = {player["number"]: player["fide_id"] for player in players}

    with open(destination_file, "a") as f2:
        for player in players:
            if not player["opponents"]:
                continue
            f2.write(f"{player['fide_id']} {len(player['opponents'])}\n")

            for opponent in player["opponents"]:
                if "result" not in opponent:
                    raise ValueError(
                        f"No result found for opponent {opponent['name']} of player {player['name']}"
                    )
                f2.write(f"{fide_id_to_player[opponent['id']]} {opponent['result']}\n")


def process_no_crosstable(
    f,
    destination_file: str,
    year: int,
    month: int,
    time_control: str,
    data_dir: Dict[str, Path],
) -> None:
    """Process tournament data without crosstable."""
    fide_data, glicko_data = get_ratings_for_period(year, month, time_control, data_dir)

    raw_players = [eval(line.strip()) for line in f]
    fide_ids = [p["fide_id"] for p in raw_players]

    # Get ratings data
    ratings_data = {
        fide_id: {
            "fide": int(fide_data[fide_id]["rating"]),
            "glicko": glicko_data.get(int(fide_id), {"rating": RATING_DEFAULT})[
                "rating"
            ],
            "rd": glicko_data.get(int(fide_id), {"rd": RD_DEFAULT})["rd"],
        }
        for fide_id in fide_ids
        if fide_id in fide_data
    }

    if not ratings_data:
        return

    m, b, average_rd = calculate_tournament_averages(ratings_data, raw_players)

    with open(destination_file, "a") as f2:
        for player in raw_players:
            average_glicko = m * float(player["RC"]) + b
            average_score = float(player["score"]) / float(player["N"])
            f2.write(f"{player['fide_id']} {player['N']}\n")
            for _ in range(int(player["N"])):
                f2.write(f"{average_glicko} {average_rd} {average_score}\n")


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

    tournament_files = list(source_dir.glob("*.txt"))
    if not tournament_files:
        logger.warning(f"No tournament files found in {source_dir}")
        exit(0)

    # Prepare arguments for parallel processing
    process_args = [
        (
            str(f),
            str(destination_dir / f.name),
            args.time_control,
            start_month,
            start_year,
            end_month,
            end_year,
            DATA_DIR,
        )
        for f in tournament_files
    ]

    # Process files in parallel
    with Pool() as pool:
        list(
            tqdm(
                pool.imap_unordered(write_fide_data_helper, process_args),
                total=len(process_args),
                desc="Processing tournaments",
            )
        )

    logger.info("Tournament data processing completed successfully")
