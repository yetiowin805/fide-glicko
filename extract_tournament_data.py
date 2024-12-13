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


def parse_crosstable(
    country: str, month: int, year: int, code: str
) -> Union[List[Player], List[MissingCrosstablePlayer]]:
    """Main function to parse a crosstable file."""
    path = get_crosstable_path(country, month, year, code)
    soup = parse_html_file(path)

    if is_missing_crosstable(soup):
        return parse_missing_crosstable(country, month, year, code)

    return parse_regular_crosstable(soup, path)


def missing_crosstable_generate_data(country, month, year, code):
    # Pad the month with a leading zero if it's less than 10
    month_str = f"{month:02d}"
    # Create the formatted string
    formatted_str = f"{year}-{month_str}"
    # Create the path
    path = os.path.join(
        "raw_tournament_data", country, formatted_str, "report", f"{code}.txt"
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


def parse_tournament_info(country, month, year, code):
    # Pad the month with a leading zero if it's less than 10
    month_str = f"{month:02d}"
    # Create the formatted string
    formatted_str = f"{year}-{month_str}"
    # Create the path
    path = os.path.join(
        "raw_tournament_data", country, formatted_str, "info", f"{code}.txt"
    )

    with open(path, encoding="utf-8") as fp:
        soup = BeautifulSoup(fp, "lxml")

        # Find all the <tr> tags
        tr_tags = soup.find_all("tr")

        date_received = None
        time_control = None

        # For each <tr> tag
        for tr in tr_tags:
            td_tags = tr.find_all("td")
            # If the first <td> tag's text is 'Date received'
            if td_tags[0].text.strip() == "Date received":
                # The second <td> tag's text is the date received
                date_received = td_tags[1].text.strip().lstrip()
                # If invalid date_received, search again for end date (only occurs a few times so inefficiency doesn't matter much)
                if date_received == "0000-00-00":
                    for tr in tr_tags:
                        td_tags = tr.find_all("td")
                        # If the first <td> tag's text is 'Date received'
                        if td_tags[0].text.strip() == "End Date":
                            # The second <td> tag's text is the date received
                            date_received = td_tags[1].text.strip().lstrip()
                            break
            # If the first <td> tag's text is 'Time Control'
            if td_tags[0].text.strip() == "Time Control":
                # The second <td> tag's text is the time control
                time_control = td_tags[1].text.strip().lstrip()
                time_control = time_control.split(":")[0]
                break

        return date_received, time_control


def get_tournament_data(country, month, year):
    # Pad the month with a leading zero if it's less than 10
    month_str = f"{month:02d}"
    # Create the formatted string
    formatted_str = f"{year}-{month_str}"
    # Create the path for tournaments
    tournaments_path = os.path.join(
        "raw_tournament_data", country, formatted_str, "tournaments.txt"
    )

    # Check if the tournaments file exists
    if os.path.isfile(tournaments_path):
        with open(tournaments_path, "r") as f:
            print(tournaments_path)
            lines = f.readlines()

        # Loop through each line in the file
        for line in lines:
            # Extract the code from the line
            code = line[:-1]

            # Define the path for the processed data
            path = os.path.join(
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
            crosstable_info = parse_crosstable(country, month, year, code)
            date_received, time_control = parse_tournament_info(
                country, month, year, code
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
    return get_tournament_data(*args)


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

    args = parser.parse_args()

    # Parse month/year
    year, month = map(int, args.month.split("-"))

    tasks = []

    for country in countries:
        tasks.append((country, month, year))

    # Number of processes to use
    num_processes = 6  # Adjust this as necessary

    # Using a multiprocessing Pool to run tasks concurrently
    with Pool(num_processes) as p:
        p.map(get_tournament_data_helper, tasks)
