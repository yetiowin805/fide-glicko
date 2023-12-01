import os
import requests
from bs4 import BeautifulSoup

def scrape_fide_data(country, month, year):

    # Pad the month with a leading zero if it's less than 10
    month_str = f"{month:02d}"
    # Create the formatted string
    formatted_str = f"country={country}&rating_period={year}-{month_str}-01"

    # Generate the URL for the specific month and year
    url = f"https://ratings.fide.com/tournament_list.phtml?moder=ev_code&{formatted_str}"
    print(url)
    # Make the HTTP request
    response = requests.get(url)

    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all 'a' elements with a href attribute that contains 'view_source.phtml'
    a_elements = soup.find_all('a', href=lambda href: href and 'view_source.phtml' in href)

    if len(a_elements):
        # Create the directory path
        dir_path = os.path.join("raw_tournament_data", country, f"{year}-{month_str}")
        os.makedirs(dir_path, exist_ok=True)

        # Save the 'a_elements' contents to a text file
        with open(os.path.join(dir_path, 'tournaments.txt'), 'w') as file:
            for element in a_elements:
                file.write(str(element) + "\n")

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

for country in countries:
    for year in range(2023,2024):
        for month in range(12,13):
            scrape_fide_data(country,month,year)
