import re
import os
import ast
from multiprocessing import Pool
from tqdm import tqdm
import itertools
from datetime import datetime


def write_fide_data(
    source_file, destination_file, time_control, month, year
):
    players = []

    # Read the source file
    with open(source_file, "r") as f:
        # Read the line and strip any leading/trailing whitespace
        date_received = f.readline().strip()

        # Remove the "Date Received: " prefix using slicing
        date_without_prefix = date_received[15:].strip()

        # Check the format based on the length of the cleaned date string and parse accordingly
        if len(date_without_prefix) != 10:  # If not in format YYYY-MM-DD, raise error
            print(source_file, destination_file, time_control, month, year)
            raise ValueError(f"Unexpected date format: {date_without_prefix}")

        date_object = datetime.strptime(date_without_prefix, "%Y-%m-%d")

        # Extract the month and year from the datetime object
        tournament_month = date_object.month
        tournament_year = date_object.year

        file_time_control = f.readline().split(":")[1].strip()

        # Early return conditions
        if file_time_control != time_control:
            return
        # For recent files we just take straight from files
        if (year < 2012 or (year == 2012 and month < 8)) and month != tournament_month:
            # mod 3 periods
            interval = 3 if year == 2008 or (year == 2009 and month < 7) else 2
            if month % interval == 2:
                return
            
            past_date = tournament_year < year or (tournament_year == year and tournament_month < month)
            future_date = tournament_year > year or (tournament_year == year and tournament_month > month)
            if (month % interval == 1 and not past_date) or (month % interval == 0 and not future_date):
                return

        for line in f:
            player = eval(line.strip())
            players.append(player)

    fide_id_to_player = {player["number"]: player["fide_id"] for player in players}

    # Append to the destination file
    with open(destination_file, "a") as f:
        for player in players:
            f.write(f"{player['fide_id']} {len(player['opponents'])}\n")

            for opponent in player["opponents"]:
                if "result" not in opponent:
                    raise ValueError(
                        f"No result found for opponent {opponent['name']} of player {player['name']}"
                    )

                f.write(f"{fide_id_to_player[opponent['id']]} {opponent['result']}\n")


def write_fide_data_helper(args):
    return write_fide_data(*args)


if __name__ == "__main__":
    countries = [
        'AFG', 'ALB', 'ALG', 'AND', 'ANG', 'ANT', 'ARG', 'ARM', 'ARU', 'AUS',
        'AUT', 'AZE', 'BAH', 'BRN', 'BAN', 'BAR', 'BLR', 'BEL', 'BIZ', 'BER',
        'BHU', 'BOL', 'BIH', 'BOT', 'BRA', 'IVB', 'BRU', 'BUL', 'BUR', 'BDI',
        'CAM', 'CMR', 'CAN', 'CPV', 'CAY', 'CAF', 'CHA', 'CHI', 'CHN', 'TPE',
        'COL', 'COM', 'CGO', 'CRC', 'CRO', 'CUB', 'CYP', 'CZE', 'COD', 'DEN',
        'DJI', 'DMA', 'DOM', 'ECU', 'EGY', 'ESA', 'ENG', 'GEQ', 'ERI', 'EST',
        'SWZ', 'ETH', 'FAI', 'FIJ', 'FIN', 'FRA', 'GAB', 'GAM', 'GEO', 'GER',
        'GHA', 'GRE', 'GRN', 'GUM', 'GUA', 'GCI', 'GUY', 'HAI', 'HON', 'HKG',
        'HUN', 'ISL', 'IND', 'INA', 'IRI', 'IRQ', 'IRL', 'IOM', 'ISR', 'ITA',
        'CIV', 'JAM', 'JPN', 'JCI', 'JOR', 'KAZ', 'KEN', 'KOS', 'KUW', 'KGZ',
        'LAO', 'LAT', 'LBN', 'LES', 'LBR', 'LBA', 'LIE', 'LTU', 'LUX', 'MAC',
        'MAD', 'MAW', 'MAS', 'MDV', 'MLI', 'MLT', 'MTN', 'MRI', 'MEX', 'MDA',
        'MNC', 'MGL', 'MNE', 'MAR', 'MOZ', 'MYA', 'NAM', 'NRU', 'NEP', 'NED',
        'AHO', 'NZL', 'NCA', 'NIG', 'NGR', 'MKD', 'NOR', 'OMA', 'PAK', 'PLW',
        'PLE', 'PAN', 'PNG', 'PAR', 'PER', 'PHI', 'POL', 'POR', 'PUR', 'QAT',
        'ROU', 'RUS', 'RWA', 'SKN', 'LCA', 'SMR', 'STP', 'KSA', 'SCO', 'SEN',
        'SRB', 'SEY', 'SLE', 'SGP', 'SVK', 'SLO', 'SOL', 'SOM', 'RSA', 'KOR',
        'SSD', 'ESP', 'SRI', 'VIN', 'SUD', 'SUR', 'SWE', 'SUI', 'SYR', 'TJK',
        'TAN', 'THA', 'TLS', 'TOG', 'TTO', 'TUN', 'TUR', 'TKM', 'UGA', 'UKR',
        'UAE', 'USA', 'URU', 'ISV', 'UZB', 'VEN', 'VIE', 'WLS', 'YEM', 'ZAM',
        'ZIM'
    ]

    tasks = []
    total_iterations = (
        17 * 12 * 3 * len(countries)
    )  # 16 years * 12 months * 3 time controls * number of countries
    progress_bar = tqdm(total=total_iterations, desc="Generating tasks")

    for year, month, time_control in itertools.product(
        range(2023, 2025), range(1, 13), ["Standard", "Rapid", "Blitz"]
    ):
        if (year == 2024 and (month > 1)) or (year == 2007 and month < 9):
            progress_bar.update(1)
            continue

        # Set data_month based on the given conditions
        if year == 2007:
            data_month = 1
        elif year == 2008 or (year == 2009 and month < 7) or (year == 2012 and month < 7) or year < 2012:
            interval = 3 if year == 2008 or (year == 2009 and month < 7) else 2
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
                        )
                    )

            progress_bar.update(1)

    progress_bar.close()

    # Using tqdm to display a progress bar
    for source_path, destination_path, time_control, month, year in tqdm(
        tasks, desc="Processing files"
    ):
        write_fide_data_helper(
            (source_path, destination_path, time_control, month, year)
        )
