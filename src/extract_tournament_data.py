import os
import requests
from bs4 import BeautifulSoup
import re
from multiprocessing import Pool
import logging
import argparse
from countries import countries
from dataclasses import dataclass
from typing import List, Dict, Optional, Union
from pathlib import Path
from bs4 import BeautifulSoup, Tag
from datetime import datetime

# Fourth command in pipeline

# Set up logging
logging.basicConfig(filename="error_log.txt", level=logging.ERROR)

# Constants
BGCOLOR_PLAYER = "#CBD8F9"
BGCOLOR_OPPONENT = "#FFFFFF"
VALID_RESULTS = {"0", "0.5", "1.0", "Forfeit"}


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


def parse_player_row(td_tags: List[Tag]) -> Optional[Player]:
    """Parse a row containing player information."""
    if len(td_tags) < 2:
        return None

    tdisa = td_tags[1].find("a")
    if not tdisa:
        return None

    return Player(
        fide_id=td_tags[0].text,
        name=tdisa.text,
        number=tdisa.get("name", ""),
        opponents=[],
    )


def parse_opponent_row(td_tags: List[Tag], path: Path) -> Optional[Opponent]:
    """Parse a row containing opponent information."""
    if len(td_tags) < 2:
        return None

    tdisa = td_tags[1].find("a")
    if not tdisa or "NOT Rated Game" in td_tags[0].get_text():
        return None

    result = parse_result(td_tags[-2], path)
    if not result or result == "Forfeit":
        return None

    return Opponent(
        name=tdisa.text, id=tdisa.get("href", "").strip("#"), result=float(result)
    )


def get_crosstable_path(
    country: str, month: int, year: int, code: str, data_dir: str
) -> Path:
    """
    Generate the file path for a crosstable based on country, month, year, and code.

    Args:
        country (str): The country code or name.
        month (int): The month number (1-12).
        year (int): The year in YYYY format.
        code (str): The tournament code.
        data_dir (str): Base directory for all data files.

    Returns:
        Path: The path to the crosstable file.
    """
    # Pad the month with a leading zero if necessary
    month_str = f"{month:02d}"
    formatted_date = f"{year}-{month_str}"

    # Define the base directory for crosstables using data_dir
    base_dir = (
        Path(data_dir)
        / "raw_tournament_data"
        / country
        / formatted_date
        / "crosstables"
    )

    # Ensure the directory exists
    base_dir.mkdir(parents=True, exist_ok=True)

    crosstable_filename = f"{code}.txt"

    # Combine the base directory and filename to get the full path
    crosstable_path = base_dir / crosstable_filename

    return crosstable_path


def parse_crosstable(
    country: str, month: int, year: int, code: str, data_dir: str
) -> Union[List[Player], List[MissingCrosstablePlayer]]:
    """Main function to parse a crosstable file."""
    path = get_crosstable_path(country, month, year, code, data_dir)
    soup = parse_html_file(path)

    if is_missing_crosstable(soup):
        return parse_missing_crosstable(country, month, year, code, data_dir)

    return parse_regular_crosstable(soup, path)


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
            print(tournaments_path)
            lines = f.readlines()

        # Loop through each line in the file
        for line in lines:
            code = line[:-1]

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

                    date_received = lines[0].strip()
                    time_control = lines[1].strip().split(":")[1].strip()
                # If any of the game results are in the content, skip to the next iteration
                if not re.match(
                    r"Date Received: \d{2}-\d{2}-\d{2}", date_received
                ) and not re.match(r"Date Received: 0000-00-00", date_received):
                    if time_control in ["Standard", "Rapid", "Blitz"]:
                        continue
                else:
                    # If not, delete the file
                    os.remove(path)

            # If the file doesn't exist or was deleted due to missing results
            crosstable_info = parse_crosstable(country, month, year, code, data_dir)
            date_received, time_control = parse_tournament_info(
                country, month, year, code, data_dir
            )

            # Create the directory if it doesn't exist
            os.makedirs(os.path.dirname(path), exist_ok=True)
            if not crosstable_info or not crosstable_info[0]:
                crosstable_info = crosstable_info[1:]
                # same as below, except we write a flag that tells the next step that we need to generate the data
                with open(path, "w") as f:
                    f.write(f"No Crosstable: True\n")
                    f.write(f"Date Received: {date_received}\n")
                    f.write(f"Time Control: {time_control}\n")
                    for element in crosstable_info:
                        f.write(f"{element}\n")
            else:
                # Write the variables to the file
                with open(path, "w") as f:
                    f.write(f"Date Received: {date_received}\n")
                    f.write(f"Time Control: {time_control}\n")
                    for element in crosstable_info:
                        f.write(f"{element}\n")


def get_tournament_data_helper(args):
    """Helper function for parallel processing."""
    return get_tournament_data(*args)


def parse_html_file(path: Path) -> BeautifulSoup:
    """Parse an HTML file into a BeautifulSoup object."""
    try:
        with open(path, encoding="utf-8") as fp:
            return BeautifulSoup(fp, "lxml")
    except Exception as x:
        logging.error(f"Unexpected result at path: {path}")
        raise x


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


def parse_result(td_tag: Tag, path: Path) -> Optional[str]:
    """Parse the result from a table cell."""
    result_tag = td_tag.find("font")
    if result_tag:
        result = result_tag.text.strip()
    else:
        result = td_tag.text.strip()
        if result[-1] == "0":
            result = "0"
        else:
            logging.error(f"Unexpected result at path: {path}, td_tags: {td_tag}")
            raise Exception(result)

    if result in ["0", "0.5", "1.0", "Forfeit"]:
        return result

    logging.error(f"Unexpected result at path: {path}, result: {result}")
    raise Exception(result)


def parse_regular_crosstable(soup: BeautifulSoup, path: Path) -> List[Player]:
    """Parse a regular tournament crosstable."""
    tr_tags = soup.find_all("tr")
    players: List[Player] = []
    current_player = None

    for tr in tr_tags:
        td_tags = tr.find_all("td")
        if not td_tags:
            continue

        bgcolor = td_tags[0].get("bgcolor")

        # New player row
        if bgcolor == BGCOLOR_PLAYER:
            if current_player is not None:
                players.append(current_player)

            player = parse_player_row(td_tags)
            if player:
                current_player = player

        # Opponent row
        elif bgcolor == BGCOLOR_OPPONENT and current_player is not None:
            opponent = parse_opponent_row(td_tags, path)
            if opponent:
                current_player.opponents.append(opponent)

    # Add the last player
    if current_player is not None:
        players.append(current_player)

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
    year, month = map(int, args.month.split("-"))

    tasks = []

    for country in countries:
        tasks.append((country, month, year, args.data_dir))

    # Number of processes to use
    num_processes = 6  # Adjust this as necessary

    # Using a multiprocessing Pool to run tasks concurrently
    with Pool(num_processes) as p:
        p.map(get_tournament_data_helper, tasks)
