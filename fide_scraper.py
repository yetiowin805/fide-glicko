import os
import requests
from bs4 import BeautifulSoup
import argparse
from countries import countries
import re

# Second command in pipeline

def scrape_fide_data(country, month, year):
    if year < 2009 and month%3 != 1:
        return
    if year == 2009 and month < 7 and month%3 != 1:
        return
    if year == 2009 and month >= 7 and month%2 != 1:
        return
    if year > 2009 and year < 2012 and month%2 != 1:
        return
    if year == 2012 and month < 7 and month%2 != 1:
        return
    # Pad the month with a leading zero if it's less than 10
    month_str = f"{month:02d}"
    # Create the formatted string
    formatted_str = f"country={country}&rating_period={year}-{month_str}-01"

    dir_path = os.path.join("raw_tournament_data", country, f"{year}-{month_str}")

    # Check if the directory path exists
    if os.path.exists(os.path.join(dir_path, 'tournaments.txt')):
        return

    # Generate the URL for the specific month and year
    url = f"https://ratings.fide.com/tournament_list.phtml?moder=ev_code&{formatted_str}"
    print(url)
    # Make the HTTP request
    response = requests.get(url)

    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')

    # Use a set to store unique hrefs
    unique_codes = set()

    # Find all <a> elements
    a_elements = soup.find_all('a', href=True)

    # Filter and add unique hrefs to the set
    for a in a_elements:
        if 'tournament_report.phtml' in a['href']:
            unique_codes.add(a['href'].split('=')[-1])

    if unique_codes:
        # Create the directory path
        os.makedirs(dir_path, exist_ok=True)

        with open(os.path.join(dir_path, 'tournaments.txt'), 'w') as file:
            for element in unique_codes:
                file.write(str(element) + "\n")

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Get FIDE tournaments from a certain month range.')
    parser.add_argument('--start_month', type=str, help='Start month for the download in YYYY-MM format', required=True)
    parser.add_argument('--end_month', type=str, help='End month for the download in YYYY-MM format', required=True)

    # Parse arguments
    args = parser.parse_args()

    # Parse start and end month/year
    start_year, start_month = map(int, args.start_month.split('-'))
    end_year, end_month = map(int, args.end_month.split('-'))

    for country in countries:
        for year in range(start_year,end_year+1):
            if start_year == end_year:
                for month in range(start_month,end_month+1):
                    scrape_fide_data(country,month,year)
            elif year == start_year:
                for month in range(start_month,13):
                    scrape_fide_data(country,month,year)
            elif year == end_year:
                for month in range(1,end_month+1):
                    scrape_fide_data(country,month,year)
            else:
                for month in range(1,13):
                    scrape_fide_data(country,month,year)
