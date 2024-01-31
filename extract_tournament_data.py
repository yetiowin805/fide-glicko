import os
import requests
from bs4 import BeautifulSoup
import re
from multiprocessing import Pool
import logging

# Set up logging
logging.basicConfig(filename='error_log.txt', level=logging.ERROR)

def parse_crosstable(country, month, year, code):
    # Pad the month with a leading zero if it's less than 10
    month_str = f"{month:02d}"
    # Create the formatted string
    formatted_str = f"{year}-{month_str}"
    # Create the path
    path = os.path.join("raw_tournament_data", country, formatted_str, "crosstables",f"{code}.txt")

    with open(path, encoding='utf-8') as fp:
        try:
            soup = BeautifulSoup(fp, 'lxml')
        except Exception as x:
            logging.error(f"Unexpected result at path: {path}")
            raise x

        # Find all the <tr> tags
        tr_tags = soup.find_all('tr')

        players_and_opponents = []

        player_info = None

        # For each <tr> tag
        for tr in tr_tags:
            td_tags = tr.find_all('td')

            bgcolor = td_tags[0].get('bgcolor')
            if len(td_tags) > 1:
                tdisa = td_tags[1].find('a')
            else:
                tdisa = None
            # If the first <td> tag's bgcolor is '#CBD8F9' and it contains an <a> tag
            if bgcolor == '#CBD8F9' and tdisa is not None:
                # If we have previous player info, add it to the list
                if player_info is not None:
                    players_and_opponents.append(player_info)
                # Start new player info
                fide_id = td_tags[0].text
                name = tdisa.text
                number = tdisa.get('name')
                player_info = {'fide_id': fide_id, 'name': name, 'number': number, 'opponents': []}
            # Else if the first <td> tag's bgcolor is '#FFFFFF' and it contains an <a> tag
            elif bgcolor == '#FFFFFF' and tdisa is not None:
                # If we have current player info, add this opponent to the list
                if player_info is not None:
                    opponent_tag = tdisa
                    opponent_name = opponent_tag.text
                    # Only add opponent if the name is not empty
                    if opponent_name.strip():
                        opponent_id = opponent_tag.get('href').strip('#')
                        result_tag = td_tags[-2].find('font')
                        if result_tag:
                            result = result_tag.text.strip()
                        else:
                            result = td_tags[-2].text.strip()  # Extract the text directly from the td tag
                            if result[-1] == '0':
                                result = '0'
                            else:
                                logging.error(f"Unexpected result at path: {path}, td_tags: {td_tags}")
                                raise Exception(result)
                        if result in ['0','0.5','1.0']:
                            player_info['opponents'].append({'name': opponent_name, 'id': opponent_id, 'result': float(result)})
                        elif result != 'Forfeit':
                            logging.error(f"Unexpected result at path: {path}, result: {result}")
                            raise Exception(result)

        # Add last player info
        if player_info is not None:
            players_and_opponents.append(player_info)

        return players_and_opponents
    
def parse_tournament_info(country, month, year, code):
    # Pad the month with a leading zero if it's less than 10
    month_str = f"{month:02d}"
    # Create the formatted string
    formatted_str = f"{year}-{month_str}"
    # Create the path
    path = os.path.join("raw_tournament_data", country, formatted_str, "info",f"{code}.txt")

    with open(path, encoding='utf-8') as fp:
        soup = BeautifulSoup(fp, 'lxml')

        # Find all the <tr> tags
        tr_tags = soup.find_all('tr')

        date_received = None
        time_control = None

        # For each <tr> tag
        for tr in tr_tags:
            td_tags = tr.find_all('td')
            # If the first <td> tag's text is 'Date received'
            if td_tags[0].text.strip() == 'Date received':
                # The second <td> tag's text is the date received
                date_received = td_tags[1].text.strip().lstrip()
                # If invalid date_received, search again for end date (only occurs a few times so inefficiency doesn't matter much)
                if date_received == "0000-00-00":
                    for tr in tr_tags:
                        td_tags = tr.find_all('td')
                        # If the first <td> tag's text is 'Date received'
                        if td_tags[0].text.strip() == 'End Date':
                            # The second <td> tag's text is the date received
                            date_received = td_tags[1].text.strip().lstrip()
                            break
            # If the first <td> tag's text is 'Time Control'
            if td_tags[0].text.strip() == 'Time Control':
                # The second <td> tag's text is the time control
                time_control = td_tags[1].text.strip().lstrip()
                time_control = time_control.split(':')[0]
                break

        return date_received,time_control

def get_tournament_data(country, month, year):
    # Pad the month with a leading zero if it's less than 10
    month_str = f"{month:02d}"
    # Create the formatted string
    formatted_str = f"{year}-{month_str}"
    # Create the path for tournaments
    tournaments_path = os.path.join("raw_tournament_data", country, formatted_str, "tournaments.txt")

    # Check if the tournaments file exists
    if os.path.isfile(tournaments_path):
        with open(tournaments_path, 'r') as f:
            print(tournaments_path)
            lines = f.readlines()

        # Loop through each line in the file
        for line in lines:
            # Extract the code from the line
            code = line[line.find("?code=")+6:line.find('"><img')]

            # Define the path for the processed data
            path = os.path.join("raw_tournament_data", country, formatted_str, "processed", f"{code}.txt")

            # Check if the file already exists
            if os.path.exists(path):
                # Read the content of the file to check for game results
                with open(path, 'r') as f:
                    content = f.read()
                    lines = content.splitlines()

                    date_received = lines[0].strip()
                    time_control = lines[1].strip().split(":")[1].strip()
                # If any of the game results are in the content, skip to the next iteration
                if not re.match(r'Date Received: \d{2}-\d{2}-\d{2}', date_received) and not re.match(r'Date Received: 0000-00-00', date_received):
                    if time_control in ["Standard", "Rapid", "Blitz"]:
                        continue
                else:
                    # If not, delete the file
                    os.remove(path)
            
            # If the file doesn't exist or was deleted due to missing results
            crosstable_info = parse_crosstable(country, month, year, code)
            date_received, time_control = parse_tournament_info(country, month, year, code)

            # Create the directory if it doesn't exist
            os.makedirs(os.path.dirname(path), exist_ok=True)

            # Write the variables to the file
            with open(path, 'w') as f:
                f.write(f"Date Received: {date_received}\n")
                f.write(f"Time Control: {time_control}\n")
                for element in crosstable_info:
                    f.write(f"{element}\n")

def get_tournament_data_helper(args):
    return get_tournament_data(*args)
        
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

    # for year in range(2008,2023):
    #     for month in range(1,13):
    #         for country in countries:
    #             tasks.append((country, month, year))
    for month in range(2,3):
        for country in countries:
            tasks.append((country, month, 2024))

    # Number of processes to use
    num_processes = 6 # Adjust this as necessary

    # Using a multiprocessing Pool to run tasks concurrently
    with Pool(num_processes) as p:
        p.map(get_tournament_data_helper, tasks)