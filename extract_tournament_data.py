import os
import requests
from bs4 import BeautifulSoup
import re
from multiprocessing import Pool
import logging
import argparse
from countries import countries

# Fourth command in pipeline

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
        if soup.find(string=lambda string: "Tournament report was updated or replaced, please view Tournament Details for more information." in string):
            return missing_crosstable_generate_data(country, month, year, code)

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
            elif bgcolor == '#FFFFFF' and tdisa is not None and "NOT Rated Game" not in tr.get_text():
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

def missing_crosstable_generate_data(country, month, year, code):
    # Pad the month with a leading zero if it's less than 10
    month_str = f"{month:02d}"
    # Create the formatted string
    formatted_str = f"{year}-{month_str}"
    # Create the path
    path = os.path.join("raw_tournament_data", country, formatted_str, "report",f"{code}.txt")

    players_info = []

    with open(path, encoding='utf-8') as fp:
        try:
            soup = BeautifulSoup(fp, 'lxml')
        except Exception as x:
            logging.error(f"Unexpected result at path: {path}")
            raise x

        colors = ["#e2e2e2", "#ffffff"]
        tr_tags = [tr for tr in soup.find_all('tr') if tr.get('bgcolor') in colors]

        for tr in tr_tags:
            td_elements = tr.find_all('td')
            # Extract information based on the position of <td> elements
            ID = td_elements[0].string.strip()
            RC = td_elements[4].string.strip()
            score = td_elements[6].string.strip()
            N = td_elements[7].string.strip()

            players_info.append({'fide_id': ID, 'RC': RC, 'score': score, 'N': N})
    return [False] + players_info

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
            code = line[:-1]

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
            if not crosstable_info or not crosstable_info[0]:
                crosstable_info = crosstable_info[1:]
                # same as below, except we write a flag that tells the next step that we need to generate the data
                with open(path, 'w') as f:
                    f.write(f"No Crosstable: True\n")
                    f.write(f"Date Received: {date_received}\n")
                    f.write(f"Time Control: {time_control}\n")
                    for element in crosstable_info:
                        f.write(f"{element}\n")
            else:
                # Write the variables to the file
                with open(path, 'w') as f:
                    f.write(f"Date Received: {date_received}\n")
                    f.write(f"Time Control: {time_control}\n")
                    for element in crosstable_info:
                        f.write(f"{element}\n")

def get_tournament_data_helper(args):
    return get_tournament_data(*args)
        
if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Get FIDE tournaments information from a certain month range.')
    parser.add_argument('--start_month', type=str, help='Start month for the download in YYYY-MM format', required=True)
    parser.add_argument('--end_month', type=str, help='End month for the download in YYYY-MM format', required=True)

    # Parse arguments
    args = parser.parse_args()

    # Parse start and end month/year
    start_year, start_month = map(int, args.start_month.split('-'))
    end_year, end_month = map(int, args.end_month.split('-'))

    tasks = []

    for country in countries:
        for year in range(start_year,end_year+1):
            if start_year == end_year:
                for month in range(start_month,end_month+1):
                    tasks.append((country, month, year))
            elif year == start_year:
                for month in range(start_month,13):
                    tasks.append((country, month, year))
            elif year == end_year:
                for month in range(1,end_month+1):
                    tasks.append((country, month, year))
            else:
                for month in range(1,13):
                    tasks.append((country, month, year))

    # Number of processes to use
    num_processes = 6 # Adjust this as necessary

    # Using a multiprocessing Pool to run tasks concurrently
    with Pool(num_processes) as p:
        p.map(get_tournament_data_helper, tasks)