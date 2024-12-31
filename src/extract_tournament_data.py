import os
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Optional, Union
from dataclasses import dataclass, asdict
from datetime import datetime
from bs4 import BeautifulSoup, Tag
from multiprocessing import Pool

from countries import countries

# Constants
BGCOLOR_PLAYER = "#CBD8F9".lower()
BGCOLOR_OPPONENT = "#FFFFFF".lower()
VALID_RESULTS = {"0", "0.5", "1.0", "Forfeit"}

# Set up logging
logging.basicConfig(
    filename="error_log.txt",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


@dataclass
class Opponent:
    name: str
    id: str
    result: float


@dataclass
class Player:
    fide_id: str
    name: str
    number: str
    opponents: List[Opponent]


@dataclass
class MissingCrosstablePlayer:
    fide_id: str
    RC: str
    score: str
    N: str


def get_crosstable_path(
    country: str, month: int, year: int, code: str, data_dir: str
) -> Path:
    """Generate the file path for a crosstable."""
    month_str = f"{month:02d}"
    formatted_date = f"{year}-{month_str}"

    base_dir = (
        Path(data_dir)
        / "raw_tournament_data"
        / country
        / formatted_date
        / "crosstables"
    )
    base_dir.mkdir(parents=True, exist_ok=True)

    return base_dir / f"{code}.txt"


def parse_html_file(path: Path) -> BeautifulSoup:
    """Parse an HTML file into a BeautifulSoup object."""
    try:
        with open(path, encoding="utf-8") as fp:
            content = fp.read()
            logger.info(f"HTML Content Snippet from {path}: {content[:1000]}")
            return BeautifulSoup(content, "lxml")
    except Exception as e:
        logger.error(f"Error parsing HTML file at {path}: {e}")
        raise


def parse_result(td_tag: Tag, path: Path) -> Optional[str]:
    """Parse the result from a table cell."""
    try:
        result_tag = td_tag.find("font")
        result = result_tag.text.strip() if result_tag else td_tag.text.strip()

        if result in VALID_RESULTS:
            return result

        raise ValueError(f"Invalid result: {result}")

    except Exception as e:
        logger.error(f"Error parsing result at {path}: {e}")
        raise


def parse_player_row(td_tags: List[Tag]) -> Optional[Player]:
    """Parse a row containing player information."""
    if len(td_tags) < 2:
        return None

    tdisa = td_tags[1].find("a")
    if not tdisa:
        return None

    return Player(
        fide_id=td_tags[0].text.strip(),
        name=tdisa.text.strip(),
        number=tdisa.get("name", "").strip(),
        opponents=[],
    )


def parse_opponent_row(td_tags: List[Tag], path: Path) -> Optional[Opponent]:
    """Parse a row containing opponent information."""

    tdisa = td_tags[1].find("a")

    if not tdisa or "NOT Rated Game" in td_tags[0].get_text():
        return None

    result = parse_result(td_tags[-2], path)
    if not result or result == "Forfeit":
        return None

    return Opponent(
        name=tdisa.text.strip(),
        id=tdisa.get("href", "").strip("#"),
        result=float(result),
    )


def parse_regular_crosstable(
    soup: BeautifulSoup, path: Path, code: str
) -> List[Player]:
    """Parse a regular tournament crosstable."""
    players_dict: Dict[str, Player] = {}
    current_player = None

    for tr in soup.find_all("tr", recursive=True):
        td_tags = tr.find_all("td")
        if not td_tags or len(td_tags) != 8:
            continue

        bgcolor = td_tags[1].get("bgcolor", "").lower() if len(td_tags) > 1 else None
        if not bgcolor:
            continue
        if bgcolor == BGCOLOR_PLAYER:
            fide_id = td_tags[0].text.strip()
            if fide_id in players_dict:
                logger.warning(f"Duplicate player {fide_id} in tournament {code}")
                current_player = None
                continue

            player = parse_player_row(td_tags)
            if player:
                players_dict[fide_id] = player
                current_player = player
            else:
                current_player = None

        elif bgcolor == BGCOLOR_OPPONENT and current_player:
            opponent = parse_opponent_row(td_tags, path)
            if opponent and not any(
                op.name == opponent.name and op.id == opponent.id
                for op in current_player.opponents
            ):
                current_player.opponents.append(opponent)

    return list(players_dict.values())


def parse_crosstable(
    country: str, month: int, year: int, code: str, data_dir: str
) -> Union[List[Player], List[MissingCrosstablePlayer]]:
    """Main function to parse a crosstable file."""
    path = get_crosstable_path(country, month, year, code, data_dir)
    soup = parse_html_file(path)

    if is_missing_crosstable(soup):
        return parse_missing_crosstable(country, month, year, code, data_dir)

    return parse_regular_crosstable(soup, path, code)


def missing_crosstable_generate_data(
    country, month, year, code, raw_tournament_data_path
):
    # Pad the month with a leading zero if it's less than 10
    month_str = f"{month:02d}"
    # Create the formatted string
    formatted_str = f"{year}-{month_str}"
    # Create the path
    path = os.path.join(
        raw_tournament_data_path, country, formatted_str, "report", f"{code}.txt"
    )

    players_info = []

    with open(path, encoding="utf-8") as fp:
        try:
            soup = BeautifulSoup(fp, "lxml")
        except Exception as x:
            logging.error(f"Unexpected result at path: {path}")
            raise x

        colors = ["#e2e2e2", "#ffffff"]
        tr_tags = [tr for tr in soup.find_all("tr") if tr.get("bgcolor") in colors]

        for tr in tr_tags:
            td_elements = tr.find_all("td")
            # Extract information based on the position of <td> elements
            ID = td_elements[0].string.strip()
            RC = td_elements[4].string.strip()
            score = td_elements[6].string.strip()
            N = td_elements[7].string.strip()

            players_info.append({"fide_id": ID, "RC": RC, "score": score, "N": N})
    return [False] + players_info


def parse_tournament_info(
    country: str, month: int, year: int, code: str, data_dir: str
):
    """Parse tournament information from info file."""
    month_str = f"{month:02d}"
    formatted_str = f"{year}-{month_str}"

    path = os.path.join(
        data_dir, "raw_tournament_data", country, formatted_str, "info", f"{code}.txt"
    )

    with open(path, encoding="utf-8") as fp:
        soup = BeautifulSoup(fp, "lxml")
        tr_tags = soup.find_all("tr")
        date_received = None
        time_control = None

        for tr in tr_tags:
            td_tags = tr.find_all("td")
            if td_tags[0].text.strip() == "Date received":
                date_received = td_tags[1].text.strip().lstrip()
                if date_received == "0000-00-00":
                    for tr in tr_tags:
                        td_tags = tr.find_all("td")
                        if td_tags[0].text.strip() == "End Date":
                            date_received = td_tags[1].text.strip().lstrip()
                            break
            if td_tags[0].text.strip() == "Time Control":
                time_control = td_tags[1].text.strip().lstrip()
                time_control = time_control.split(":")[0]
                break

        return date_received, time_control


def get_tournament_data(country: str, month: int, year: int, data_dir: str):
    """Process tournament data for a given country and month."""
    # Pad the month with a leading zero if it's less than 10
    month_str = f"{month:02d}"
    formatted_str = f"{year}-{month_str}"

    # Create the path for tournaments
    tournaments_path = os.path.join(
        data_dir, "raw_tournament_data", country, formatted_str, "tournaments.txt"
    )

    # Check if the tournaments file exists
    if os.path.isfile(tournaments_path):
        with open(tournaments_path, "r") as f:
            logger.info(f"Processing tournaments from: {tournaments_path}")
            lines = f.readlines()

        # Loop through each line in the file
        for line in lines:
            code = line.strip()

            if not code:
                logger.warning(
                    f"Empty tournament code found in {tournaments_path}. Skipping."
                )
                continue

            # Define the path for the processed data
            path = os.path.join(
                data_dir,
                "raw_tournament_data",
                country,
                formatted_str,
                "processed",
                f"{code}.txt",
            )

            # Check if the file already exists
            if os.path.exists(path):
                # Read the content of the file to check for game results
                with open(path, "r") as f:
                    content = f.read()
                    lines = content.splitlines()

                    if len(lines) < 2:
                        logger.warning(
                            f"Incomplete data in {path}. Deleting and reprocessing."
                        )
                        os.remove(path)
                        should_process = True
                    else:
                        date_received = lines[0].strip()
                        time_control = (
                            lines[1].strip().split(":")[1].strip()
                            if ":" in lines[1]
                            else None
                        )

                        # Check if the tournament has already been processed with valid data
                        if date_received != "0000-00-00" and time_control in [
                            "Standard",
                            "Rapid",
                            "Blitz",
                        ]:
                            logger.info(
                                f"Tournament {code} already processed. Skipping."
                            )
                            should_process = False
                        else:
                            logger.info(
                                f"Invalid or missing data in {path}. Deleting and reprocessing."
                            )
                            os.remove(path)
                            should_process = True

            else:
                should_process = True

            if should_process:
                # Parse crosstable
                try:
                    crosstable_info = parse_crosstable(
                        country, month, year, code, data_dir
                    )
                except Exception as e:
                    logger.error(f"Error parsing crosstable for tournament {code}: {e}")
                    continue

                # Parse tournament info
                try:
                    date_received, time_control = parse_tournament_info(
                        country, month, year, code, data_dir
                    )
                except Exception as e:
                    logger.error(f"Error parsing tournament info for {code}: {e}")
                    date_received, time_control = "0000-00-00", "Unknown"

                # Create the directory if it doesn't exist
                if not crosstable_info:
                    logger.error(f"No crosstable info for tournament {code}. Skipping.")
                    continue

                os.makedirs(os.path.dirname(path), exist_ok=True)

                if isinstance(crosstable_info, list) and not isinstance(
                    crosstable_info[0], bool
                ):
                    # Regular crosstable
                    with open(path, "w") as f:
                        f.write(f"Date Received: {date_received}\n")
                        f.write(f"Time Control: {time_control}\n")
                        for player in crosstable_info:
                            f.write(f"{asdict(player)}\n")
                elif isinstance(crosstable_info, list) and isinstance(
                    crosstable_info[0], bool
                ):
                    # Missing crosstable
                    crosstable_info = crosstable_info[1:]
                    with open(path, "w") as f:
                        f.write(f"No Crosstable: True\n")
                        f.write(f"Date Received: {date_received}\n")
                        f.write(f"Time Control: {time_control}\n")
                        for element in crosstable_info:
                            f.write(f"{asdict(element)}\n")
                else:
                    logger.error(
                        f"Unexpected crosstable_info format for tournament {code}. Skipping."
                    )


def get_tournament_data_helper(args):
    """Helper function for parallel processing."""
    return get_tournament_data(*args)


def is_missing_crosstable(soup: BeautifulSoup) -> bool:
    """Check if the crosstable is missing based on the soup content."""
    return bool(
        soup.find(
            string=lambda string: "Tournament report was updated or replaced, please view Tournament Details for more information."
            in string
        )
    )


def parse_missing_crosstable(
    country: str, month: int, year: int, code: str, data_dir: str
) -> List[MissingCrosstablePlayer]:
    """Parse tournament data when crosstable is missing."""
    # Get path to report file
    month_str = f"{month:02d}"
    formatted_str = f"{year}-{month_str}"
    path = (
        Path(data_dir)
        / "raw_tournament_data"
        / country
        / formatted_str
        / "report"
        / f"{code}.txt"
    )

    with open(path, encoding="utf-8") as fp:
        try:
            soup = BeautifulSoup(fp, "lxml")
        except Exception as x:
            logging.error(f"Unexpected result at path: {path}")
            raise x

        # Find rows with specific background colors
        colors = ["#e2e2e2", "#ffffff"]
        tr_tags = [tr for tr in soup.find_all("tr") if tr.get("bgcolor") in colors]

        players = []
        for tr in tr_tags:
            td_elements = tr.find_all("td")
            players.append(
                MissingCrosstablePlayer(
                    fide_id=td_elements[0].string.strip(),
                    RC=td_elements[4].string.strip(),
                    score=td_elements[6].string.strip(),
                    N=td_elements[7].string.strip(),
                )
            )
        return players


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get FIDE tournaments information for a specific month."
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
    try:
        year, month = map(int, args.month.split("-"))
    except ValueError:
        logger.error("Invalid month format. Expected YYYY-MM.")
        raise

    tasks = []

    for country in countries:
        tasks.append((country, month, year, args.data_dir))

    # Number of processes to use
    num_processes = 6  # Adjust this as necessary

    # Using a multiprocessing Pool to run tasks concurrently
    with Pool(num_processes) as p:
        p.map(get_tournament_data_helper, tasks)
