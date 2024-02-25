import re
import os
import ast
from multiprocessing import Pool
from tqdm import tqdm
import itertools
import numpy as np
from datetime import datetime
import argparse
from countries import countries

# Fourth command in pipeline

# Dictionary of dictionaries, query month then FIDE ID
fide_ratings = {}
glicko_ratings = {}

def write_fide_data(
    source_file, destination_file, time_control, month, year, end_month, end_year
):  
    players = []

    # Read the source file
    with open(source_file, "r") as f:
        first_line = f.readline().strip()
        if "No Crosstable" in first_line:
            date_received = f.readline().strip()
        else:
            # Read the line and strip any leading/trailing whitespace
            date_received = first_line
        # Remove the "Date Received: " prefix using slicing
        date_without_prefix = date_received[15:].strip()

        # Check the format based on the length of the cleaned date string and parse accordingly
        if len(date_without_prefix) != 10:  # If not in format YYYY-MM-DD, raise error
            if date_without_prefix == "Not submitted":
                return
            print(source_file, destination_file, time_control, month, year)
            raise ValueError(f"Unexpected date format: {date_without_prefix}")

        date_object = datetime.strptime(date_without_prefix, "%Y-%m-%d")

        # Extract the month and year from the datetime object
        tournament_month = date_object.month
        tournament_year = date_object.year
        if tournament_year > end_year or (tournament_year == end_year and tournament_month > end_month):
            return

        file_time_control = f.readline().split(":")[1].strip()

        # Early return conditions
        if file_time_control != time_control:
            return
        # For recent files we just take straight from files
        if (year < 2012 or (year == 2012 and month < 8)) and month != tournament_month:
            # mod 3 periods
            interval = 3 if year <= 2008 or (year == 2009 and month < 7) else 2
            if month % interval == 2:
                return
            
            past_date = tournament_year < year or (tournament_year == year and tournament_month < month)
            future_date = tournament_year > year or (tournament_year == year and tournament_month > month)
            if (month % interval == 1 and not past_date) or (month % interval == 0 and not future_date):
                return
        if "No Crosstable" in first_line:
            # TODO:
            # Get all player FIDE IDs
            # Get all play FIDE and glicko ratings
            # Fit a linear transformation between ratings
            # Estimate average glicko rating with transformation
            # Also get all player deviations, take average
            # Pretend each player plays N games against the estimated average rating
            # With each score being score/N
            if fide_ratings.get(f"{year}-{month}") is None:
                # TODO: get fide ratings from file
                player_info_path = f"./player_info/processed/{year:04d}-{month:02d}.txt"
            
                temp_month, temp_year = month, year
                while not os.path.exists(player_info_path):
                    # If the player_info file for the current month doesn't exist, move to the previous month
                    if temp_month == 1:
                        temp_year -= 1
                        temp_month = 12
                    else:
                        temp_month -= 1
                    player_info_path = f"./player_info/processed/{temp_year:04d}-{temp_month:02d}.txt"
                with open(player_info_path, 'r', encoding='utf-8', errors='replace') as f2:
                    temp_players = {}
                    for line in f2:
                        temp_player = eval(line.strip())
                        temp_players[temp_player["id"]] = temp_player
                fide_ratings[f"{year}-{month}"] = temp_players
            if glicko_ratings.get(f"{year}-{month}") is None:
                with open(f"./rating_lists/{time_control}/{year:04d}-{month:02d}.txt") as f2:
                    temp_players = {}
                    for line in f2:
                        components = line.split()
                        temp_player = dict(zip(["id","rating","rd","volatility"],[int(components[0])]+[float(x) for x in components[1:]]))
                        temp_players[temp_player["id"]] = temp_player
                glicko_ratings[f"{year}-{month}"] = temp_players
            raw_players = []
            fide_ids = []
            for line in f:
                # Get all player FIDE IDs
                player = eval(line.strip())
                raw_players.append(player)
                fide_ids.append(player["fide_id"])
            
            tournament_fide_ratings = [int(fide_ratings[f"{year}-{month}"].get(fide_id)["rating"]) for fide_id in fide_ids
                                        if fide_ratings[f"{year}-{month}"].get(fide_id) is not None]
            tournament_glicko_ratings = [glicko_ratings[f"{year}-{month}"].get(int(fide_id), {"rating":1500})["rating"] for fide_id in fide_ids
                                        if fide_ratings[f"{year}-{month}"].get(fide_id) is not None]
            tournament_rds = [glicko_ratings[f"{year}-{month}"].get(int(fide_id), {"rd":350})["rd"] for fide_id in fide_ids
                                        if fide_ratings[f"{year}-{month}"].get(fide_id) is not None]
            if tournament_fide_ratings:
                m, b = np.polyfit(tournament_fide_ratings, tournament_glicko_ratings, 1)
                average_rd = sum(tournament_rds)/len(tournament_rds)
            else:
                m, b = 0, 1500
                average_rd = 350

            with open(destination_file, "a") as f2:
                for player in raw_players:
                    average_glicko = m*float(player["RC"])+b
                    average_score = float(player["score"])/float(player["N"])
                    f2.write(f"{player['fide_id']} {player['N']}\n")
                    for _ in range(int(player["N"])):
                        f2.write(f"{average_glicko} {average_rd} {average_score}\n")
                
        else:
            for line in f:
                player = eval(line.strip())
                players.append(player)

            fide_id_to_player = {player["number"]: player["fide_id"] for player in players}

            # Append to the destination file
            with open(destination_file, "a") as f2:
                for player in players:
                    if not player['opponents']:
                        continue
                    f2.write(f"{player['fide_id']} {len(player['opponents'])}\n")

                    for opponent in player["opponents"]:
                        if "result" not in opponent:
                            raise ValueError(
                                f"No result found for opponent {opponent['name']} of player {player['name']}"
                            )
                        f2.write(f"{fide_id_to_player[opponent['id']]} {opponent['result']}\n")


def write_fide_data_helper(args):
    return write_fide_data(*args)


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Download FIDE player information.')
    parser.add_argument('--start_month', type=str, help='Start month for the download in YYYY-MM format', required=True)
    parser.add_argument('--end_month', type=str, help='End month for the download in YYYY-MM format', required=True)
    # Parse arguments
    args = parser.parse_args()

    # Parse start and end month/year
    start_year, start_month = map(int, args.start_month.split('-'))
    end_year, end_month = map(int, args.end_month.split('-'))
    tasks = []

    # Calculate total iterations considering the range of dates and time controls
    months_difference = (end_year - start_year) * 12 + end_month - start_month + 1
    total_iterations = months_difference * len(countries) * 3  # For each time control
    progress_bar = tqdm(total=total_iterations, desc="Generating tasks")

    for year, month, time_control in itertools.product(
        range(start_year, end_year+1), range(1, 13), ["Standard", "Rapid", "Blitz"]
    ):
        if (year == start_year and month < start_month) or (year == end_year and month > end_month):
            progress_bar.update(1)
            continue

        # Set data_month based on the given conditions
        if year <= 2008 or (year == 2009 and month < 7) or (year == 2012 and month < 7) or year < 2012:
            interval = 3 if year <= 2008 or (year == 2009 and month < 7) else 2
            data_month = (12 + ((month - 1) // interval + 1) * interval) % 12 + 1
        else:
            data_month = month % 12 + 1

        # Pad the month with a leading zero if it's less than 10
        data_month_str = f"{data_month:02d}"
        # Create the formatted string
        data_formatted_str = (
            f"{year+1 if month > data_month else year}-{data_month_str}"
        )

        # Pad the month with a leading zero if it's less than 10
        month_str = f"{month:02d}"
        # Create the formatted string
        formatted_str = f"{year}-{month_str}"

        destination_path = os.path.join(
            "clean_numerical", formatted_str, time_control, "games.txt"
        )

        # Ensure the directory structure exists
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)

        # Delete the destination file if it exists
        if os.path.exists(destination_path):
            os.remove(destination_path)
            open(destination_path, "a").close()
        else:
            open(destination_path, "w").close()

        for country in countries:
            # Define the directory for the processed data
            directory_path = os.path.join("raw_tournament_data", country, data_formatted_str, "processed")
            # Check if the directory exists
            if os.path.exists(directory_path):
                # Iterate through all files in the directory
                for file_name in os.listdir(directory_path):
                    source_path = os.path.join(directory_path, file_name)
                    # Append the task
                    tasks.append(
                        (
                            source_path,
                            destination_path,
                            time_control,
                            month,
                            year,
                            end_month,
                            end_year,
                        )
                    )

            progress_bar.update(1)

    progress_bar.close()

    # Using tqdm to display a progress bar
    for source_path, destination_path, time_control, month, year, end_month, end_year in tqdm(
        tasks, desc="Processing files"
    ):
        write_fide_data_helper(
            (source_path, destination_path, time_control, month, year, end_month, end_year)
        )
