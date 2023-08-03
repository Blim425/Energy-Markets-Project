"""
Author: Bryce

Desc:
This script web scrapes electricity price in CSVs from EMI. Stores the CSVs to local files
"""

import requests
from requests.auth import HTTPBasicAuth
import os
from bs4 import BeautifulSoup

def get_url_paths(url, ext=''):
    response = requests.get(url)
    # response = requests.get(url, auth=HTTPBasicAuth('username', 'password'))
    if response.ok:
        response_text = response.text
    else:
        return response.raise_for_status()
    soup = BeautifulSoup(response_text, 'html.parser')
    # Note must get all hrefs that are not Nonetype. Also remove all duplicates
    CSV_links = list(set(node.get('href') for node in soup.find_all('a') if node.get('href') and node.get('href').endswith(ext)))
    # Remove interim reports
    CSV_links = [string for string in CSV_links if "Prices_I" not in string]
    # Sort
    CSV_links.sort()
    CSV_links =  [url[0:26] + link for link in CSV_links]

    return CSV_links
def main():
    url = "https://www.emi.ea.govt.nz/Wholesale/Datasets/DispatchAndPricing/FinalEnergyPrices"
    ext = '.csv'
    print('Establishing Connection...')
    result = get_url_paths(url, ext)
    print('Getting file links...')
    for file in result:
        f_name = file[83:-4]

        if not os.path.exists(f'C:/Users/bryce/OneDrive - The University of Auckland/Electricity data/{f_name}.csv'):
            # The file does not exists. Can save
            r = requests.get(file)
            # r = requests.get(file, auth=HTTPBasicAuth('username', 'password'))
            with open(f'C:/Users/bryce/OneDrive - The University of Auckland/Electricity data/{f_name}.csv', 'wb') as f:
                f.write(r.content)
                print(f"Writing: {f_name}")
        else:
            print(f"File already exists. Skipping {f_name}.")

if __name__ == '__main__':
    print("Starting...")
    main()
    print("Finished")