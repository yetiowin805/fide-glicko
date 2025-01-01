import re
import ast
from tqdm import tqdm
import numpy as np
from datetime import datetime
import argparse
from countries import countries
import logging
from pathlib import Path
from typing import Dict, Tuple, Optional
from multiprocessing import Pool, Lock, Manager

# Configure logging
logging.basicConfig(
    filename="collect_player_data.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global rating caches
fide_ratings: Dict[int, Dict] = {}
glicko_ratings: Dict[int, Dict] = {}

# Configuration constants
RATING_DEFAULT = 1500
RD_DEFAULT = 350

# Add global lock
lock: Optional[Lock] = None


def initializer_process(l: Lock):
    global lock
    lock = l


def setup_data_dirs(base_dir: str) -> Dict[str, Path]:
    return {
        "player_info": Path(base_dir) / "player_info" / "processed",
        "rating_lists": Path(base_dir) / "rating_lists",
        "raw_tournament": Path(base_dir) / "raw_tournament_data",
        "clean_numerical": Path(base_dir) / "clean_numerical",
    }


def get_player_info(year: int, month: int, data_dir: Dict[str, Path]) -> Path:
    temp_year, temp_month = year, month
    while temp_year >= 2000:
        player_info_path = (
            data_dir["player_info"] / f"{temp_year:04d}-{temp_month:02d}.txt"
        )
        if player_info_path.exists():
            return player_info_path
        temp_year, temp_month = (
            (temp_year - 1, 12) if temp_month == 1 else (temp_year, temp_month - 1)
        )
    logger.error("No available player_info files found.")
    return Path()


def get_ratings_for_period(
    year: int, month: int, time_control: str, data_dir: Dict[str, Path]
) -> Tuple[Dict[int, Dict], Dict[int, Dict]]:
    fide, glicko = {}, {}

    player_info_path = get_player_info(year, month, data_dir)
    logger.info(f"Player info path: {player_info_path}")
    if player_info_path.exists():
        try:
            with open(player_info_path, "r", encoding="utf-8", errors="replace") as f:
                fide = {
                    int(player["id"]): player
                    for player in map(ast.literal_eval, f)
                    if "id" in player
                }
            logger.info(f"Loaded FIDE ratings for {year}-{month:02d}")
        except Exception as e:
            logger.error(f"Error reading FIDE ratings from {player_info_path}: {e}")

    rating_path = (
        data_dir["rating_lists"] / time_control / f"{year:04d}-{month:02d}.txt"
    )
    logger.info(f"Glicko rating path: {rating_path}")
    if rating_path.exists():
        try:
            with open(rating_path, "r", encoding="utf-8") as f:
                glicko = {
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
            logger.info(f"Loaded Glicko ratings for {year}-{month:02d}")
        except Exception as e:
            logger.error(f"Error reading Glicko ratings from {rating_path}: {e}")

    return fide, glicko


def get_target_date(year: int, month: int) -> Tuple[int, int]:
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

    return year, target_month


def is_valid_tournament_date(
    target_year: int, target_month: int, tournament_year: int, tournament_month: int
) -> bool:
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
    date_without_prefix = (
        date_line[15:].strip()
        if date_line.startswith("Date Received:")
        else date_line.strip()
    )

    if date_without_prefix == "Not submitted":
        return None, False

    try:
        date_object = datetime.strptime(date_without_prefix, "%Y-%m-%d")
        return date_object, True
    except ValueError:
        logger.error(f"Invalid date format: {date_without_prefix}")
        return None, False


def write_fide_data(
    source_file: str,
    time_control: str,
    month: int,
    year: int,
    data_dir: Dict[str, Path],
) -> None:
    global lock, fide_ratings, glicko_ratings
    try:
        with open(source_file, "r", encoding="utf-8") as f:
            date_line = f.readline().strip()
            date_obj, has_valid_date = parse_tournament_date(date_line)
            if not has_valid_date:
                logger.info(f"Date not submitted or invalid in file: {source_file}")
                return

            if not is_valid_tournament_date(year, month, date_obj.year, date_obj.month):
                logger.info(
                    f"Tournament date {date_obj.strftime('%Y-%m-%d')} is not valid for target date {year}-{month:02d} in file {source_file}"
                )
                return

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
            destination_file.parent.mkdir(parents=True, exist_ok=True)

            if "opponents" in first_player:
                players = [first_player]
                for line_number, line in enumerate(f, start=3):
                    if not line.strip():
                        continue
                    try:
                        player_data = ast.literal_eval(line.strip())
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

                number_to_fide_id = {
                    player["number"]: player["fide_id"] for player in players
                }

                with lock:
                    with open(destination_file, "a", encoding="utf-8") as f2:
                        for player in players:
                            fide_id = player["fide_id"]
                            opponents = player["opponents"]
                            # Filter valid opponents first
                            valid_opponents = []
                            for opponent in opponents:
                                opponent_number = opponent.get("id")
                                opponent_fide_id = number_to_fide_id.get(
                                    opponent_number
                                )
                                if not opponent_number or not opponent_fide_id:
                                    logger.warning(
                                        f"Invalid opponent (number={opponent_number}) for player {fide_id} in file {source_file}"
                                    )
                                    continue
                                valid_opponents.append(
                                    (opponent_fide_id, opponent.get("result", 0.0))
                                )

                            # Write only if there are valid opponents
                            if valid_opponents:
                                f2.write(f"{fide_id} {len(valid_opponents)}\n")
                                for (
                                    opponent_fide_id,
                                    opponent_result,
                                ) in valid_opponents:
                                    f2.write(f"{opponent_fide_id} {opponent_result}\n")
            elif all(k in first_player for k in ("RC", "score", "N")):
                players = [first_player]
                for line_number, line in enumerate(f, start=3):
                    if not line.strip():
                        continue
                    try:
                        player_data = ast.literal_eval(line.strip())
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

                if not fide_ratings and not glicko_ratings:
                    fide, glicko = get_ratings_for_period(
                        year, month, time_control, data_dir
                    )
                    fide_ratings.update(fide)
                    glicko_ratings.update(glicko)

                fide_ids = [player["fide_id"] for player in players]
                tournament_fide_ratings = [
                    fide_ratings.get(fide_id, {}).get("rating", RATING_DEFAULT)
                    for fide_id in fide_ids
                ]
                tournament_glicko_ratings = [
                    glicko_ratings.get(fide_id, {}).get("rating", RATING_DEFAULT)
                    for fide_id in fide_ids
                ]
                tournament_rds = [
                    glicko_ratings.get(fide_id, {}).get("rd", RD_DEFAULT)
                    for fide_id in fide_ids
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

                with lock:
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
    except Exception as e:
        logger.error(f"Error processing {source_file}: {e}")


def write_fide_data_helper(args):
    return write_fide_data(*args)


if __name__ == "__main__":
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

    args = parser.parse_args()

    def validate_month_format(month_str: str) -> bool:
        try:
            datetime.strptime(month_str, "%Y-%m")
            return True
        except ValueError:
            return False

    if not validate_month_format(args.month):
        logger.error("Invalid month format. Use YYYY-MM")
        exit(1)

    year, month = map(int, args.month.split("-"))
    target_year, target_month = get_target_date(year, month)
    DATA_DIR = setup_data_dirs(args.data_dir)

    for directory in DATA_DIR.values():
        directory.mkdir(parents=True, exist_ok=True)

    source_dir = DATA_DIR["raw_tournament"]
    destination_dir = DATA_DIR["clean_numerical"]
    time_control = args.time_control

    tournament_files = []
    destination_paths_set = set()

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
        tasks.append(
            (
                str(file_path),
                time_control,
                month,
                year,
                DATA_DIR,
            )
        )

    if not tasks:
        logger.warning("No tournament files found within the specified date range.")
        exit(0)

    destination_path = (
        destination_dir / f"{year:04d}-{month:02d}" / time_control / "games.txt"
    )
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with open(destination_path, "w", encoding="utf-8") as f:
        pass
    logger.info(f"Cleared/created destination file: {destination_path}")

    logger.info(
        f"Processing {len(tasks)} tournament files with '{time_control}' time control for {args.month}."
    )

    # Add manager and lock creation back
    manager = Manager()
    shared_lock = manager.Lock()

    with Pool(initializer=initializer_process, initargs=(shared_lock,)) as pool:
        list(
            tqdm(
                pool.imap_unordered(write_fide_data_helper, tasks),
                total=len(tasks),
                desc="Processing files",
            )
        )

    logger.info("FIDE player data collection completed successfully.")
