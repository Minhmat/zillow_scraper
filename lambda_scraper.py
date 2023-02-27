import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

base_url = 'https://www.zillow.com/'
headers = {
    'authority': 'www.zillow.com',
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9,vi;q=0.8,vi-VN;q=0.7',
    'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
}


def scraper():
    with requests.session() as s:
        city = 'new-york-ny/'
        urls = [f'{base_url}{city}{page}_p/' for page in range(1, 5)]
        responses = [s.get(url=url, headers=headers) for url in urls]
    soups = [BeautifulSoup(response.content, 'html.parser') for response in responses]
    return soups


def make_table(soup):
    data = json.loads(
        soup.select_one("script[data-zrr-shared-data-key]")
        .contents[0].strip("!<>-")
    )
    items = data["cat1"]["searchResults"]["listResults"]
    rows = []
    for item in items:
        row = {'statusText': item['statusText'],
               'price': item['unformattedPrice'],
               'addressStreet': item['addressStreet'],
               'addressCity': item['addressCity'],
               'addressState': item['addressState'],
               'beds': item['beds'],
               'baths': item['baths'],
               'area': item['area'],
               'url': item['detailUrl']}
        rows.append(row)
    df = pd.DataFrame(rows)
    return df


def lambda_handler(event, context):
    logger.info('Getting raw data...')
    soups = scraper()
    logger.info('Making table...')
    dfs = [make_table(soup) for soup in soups]
    df = pd.concat(dfs).reset_index().drop(columns='index')
    today = datetime.now().strftime('%Y%m%d')
    df.to_csv(f's3://zillow-raw/data/zillow_{today}.csv', index=False)
    return 'Got the data'
