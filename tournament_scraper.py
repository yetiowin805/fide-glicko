import os
import requests
from bs4 import BeautifulSoup
import re
from multiprocessing import Pool

def scrape_tournament_data(country, month, year):
    # Pad the month with a leading zero if it's less than 10
    month_str = f"{month:02d}"
    # Create the formatted string
    formatted_str = f"{year}-{month_str}"
    # Create the path
    path = os.path.join("raw_tournament_data", country, formatted_str, "tournaments.txt")

    # Check if the file exists
    if os.path.isfile(path):
        print(path)
        with open(path, 'r') as f:
            lines = f.readlines()
        
        # Define base URL
        base_url = "https://ratings.fide.com/"

        # Loop through each line in the file
        for line in lines:
            # Extract the code from the line
            code = line[line.find("?code=")+6:line.find('"><img')]

            # Create the file path for the new file
            new_path = os.path.join(os.path.dirname(path), 'info', f'{code}.txt')
            
            # Check if the new file path exists and is not empty
            if os.path.exists(new_path) and os.path.getsize(new_path) > 0:
                # File exists and is not empty, skip this iteration
                continue

            # If the file doesn't exist or is empty, fetch data from the URL
            # Form the complete URL
            url = base_url + 'tournament_details.phtml?event=' + code

            # Make the HTTP request
            response = requests.get(url)

            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')

            # Make sure the directory exists
            os.makedirs(os.path.dirname(new_path), exist_ok=True)
            
            # Write the contents of 'soup' into the file
            with open(new_path, 'w', encoding='utf-8') as f:
                f.write(str(soup))

        # Loop through each line in the file
        for line in lines:
            # Extract the code from the line
            code = line[line.find("?code=")+6:line.find('"><img')]

            # Create the file path for the new file
            new_path = os.path.join(os.path.dirname(path), 'crosstables', f'{code}.txt')
            
            # Check if the new file path exists and is not empty
            if os.path.exists(new_path) and os.path.getsize(new_path) > 0:
                # File exists and is not empty, skip this iteration
                continue

            # If the file doesn't exist or is empty, fetch data from the URL
            # Form the complete URL
            url = base_url + 'view_source.phtml?code=' + code

            # Make the HTTP request
            response = requests.get(url)

            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')

            # Make sure the directory exists
            os.makedirs(os.path.dirname(new_path), exist_ok=True)
            
            # Write the contents of 'soup' into the file
            with open(new_path, 'w', encoding='utf-8') as f:
                f.write(str(soup))

def scrape_country_month_year(args):
    return scrape_tournament_data(*args)


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


    # Create a list to hold all tasks
    tasks = []

    # for year in range(2008,2023):
    #     for month in range(1,13):
    #         for country in countries:
    #             tasks.append((country, month, year))
    for month in range(10,11):
        for country in countries:
            tasks.append((country, month, 2023))

    # Number of processes to use
    num_processes = 6 # Adjust this as necessary

    # Using a multiprocessing Pool to run tasks concurrently
    with Pool(num_processes) as p:
        p.map(scrape_country_month_year, tasks)