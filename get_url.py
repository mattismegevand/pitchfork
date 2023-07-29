import requests
import pandas as pd

from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from os.path import isfile
from sys import argv


def fetch(args):
  ''' Fetch a single url '''
  url, session = args
  response = session.get(url)
  page_number = url.split('=')[-1]
  soup = BeautifulSoup(response.text, 'lxml')
  error = soup.find('div', {'class': 'error-page'})
  if error:
    print(f'Error page: {url} does not exist')
    return []
  print('.', end='', flush=True)
  return [(page_number, f"https://pitchfork.com{e['href']}") for e in soup.find_all('a', {'class': 'review__link'})]

def get_urls(start, end):
  ''' Return a list of urls from the Pitchfork reviews page '''
  urls = [f'https://pitchfork.com/reviews/albums/?page={i}' for i in range(start, end+1)]
  reviews = []
  session = requests.Session()
  with ThreadPoolExecutor(max_workers=5) as executor:
    for result in executor.map(fetch, ((url, session) for url in urls)):
      reviews.extend(result)
  print()
  return reviews

def insert_into_df(data):
  ''' Insert data into a pandas dataframe '''
  df = pd.DataFrame(data, columns=['page', 'url'])
  df.drop_duplicates(subset='url', keep='first', inplace=True)
  return df

def main():
  start, end = int(argv[1]), int(argv[2])
  print(f'Fetching urls from pages {start} to {end}')
  data = get_urls(start, end)
  print(f'Fetched {len(data)} urls')
  df = insert_into_df(data)

  print(f'Writing to urls.csv')
  if isfile('urls.csv'):
    df_existing = pd.read_csv('urls.csv')
    df_combined = pd.concat([df_existing, df])
  else:
    df_combined = df

  df_combined.drop_duplicates(subset='url', keep='first', inplace=True)
  df_combined.to_csv('urls.csv', index=False)
  print('Done')

if __name__ == '__main__':
  main()
