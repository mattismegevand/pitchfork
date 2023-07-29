import re
import requests
import sqlite3
import pandas as pd

from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup, SoupStrainer
from sys import argv

regexes = {
  'artist': re.compile(r'SplitScreenContentHeaderArtist-\w*'),
  'album': re.compile(r'SplitScreenContentHeaderHed-\w*'),
  'year_released': re.compile(r'SplitScreenContentHeaderReleaseYear-\w*'),
  'rating': re.compile(r'Rating-\w*'),
  'small_text': re.compile(r'SplitScreenContentHeaderDekDown-\w*'),
  'review': re.compile(r'body__inner-container'),
  'reviewer': re.compile(r'BylineName'),
  'genre': re.compile(r'SplitScreenContentHeaderInfoSlice-\w*'),
  'label': re.compile(r'SplitScreenContentHeaderInfoSlice-\w*'),
  'reviewed': re.compile(r'SplitScreenContentHeaderInfoSlice-\w*'),
  'album_art_url': re.compile(r'SplitScreenContentHeaderImage-\w*'),
}

def fetch(args):
  ''' Fetch a single url and return a dictionary of data from a Pitchfork review '''
  url, session = args
  response = session.get(url)
  if response.status_code == 200:
    soup_strainer = SoupStrainer('article', {'data-testid': 'ReviewPageArticle'})
    soup = BeautifulSoup(response.content, 'lxml', parse_only=soup_strainer)
    if soup.find('article', {'data-testid': 'ReviewPageArticle'}) is None:
      with open('not_done.txt', 'a') as f:
        f.write(url + '\n')
      return None
    print('.', end='', flush=True)
    result = data_from_soup(soup)
    if result is None:
      with open('not_done.txt', 'a') as f:
        f.write(url + '\n')
    return result
  else:
    with open('errors.txt', 'a') as f:
      f.write(url + '\n')
    return None

def get_reviews(urls):
  ''' Return a list of review data dictionaries from the provided urls '''
  reviews = []
  session = requests.Session()
  with ThreadPoolExecutor() as executor:
    for result in executor.map(fetch, ((url, session) for url in urls)):
      if result:  # Check if result is not None
        reviews.append(result)
  print()
  return reviews

def data_from_soup(soup):
  ''' Return a dictionary of data from a Pitchfork review '''
  artist = soup.find('div', {'class': regexes['artist']}).text.strip()
  album = soup.find('h1', {'class': regexes['album']}).text.strip()
  year_released = soup.find('time', {'class': regexes['year_released']})
  if year_released:
    year_released = int(year_released.text.strip())
  else:
    return None
  rating = float(soup.find('p', {'class': regexes['rating']}).text.strip())
  small_text = soup.find('div', {'class': regexes['small_text']})
  small_text = small_text.text.strip() if small_text else 'N/A'
  review = "".join(e.text for e in soup.find('div', {'class': regexes['review']}).descendants if e.name == 'p')
  reviewer = soup.find('span', {'data-testid': regexes['reviewer']})
  reviewer = reviewer.text.strip()[3:] if reviewer else 'N/A'
  misc = [e.text for e in soup.find('div', {'class': regexes['genre']}).descendants if e.name == 'li']
  misc = {'genre': 'N/A', 'label': 'N/A', 'reviewed': 'N/A'} | {e.split(':')[0].strip().lower(): e.split(':')[1].strip() for e in misc}
  album_art_url = soup.find('source', {'media': '(max-width: 767px)'})
  album_art_url = album_art_url['srcset'].split(',')[-2].strip() if album_art_url else 'N/A'
  return {
    'artist': artist, 'album': album, 'year_released': year_released,
    'rating': rating, 'small_text': small_text, 'review': review,
    'reviewer': reviewer, 'genre': misc['genre'], 'label': misc['label'],
    'reviewed': misc['reviewed'], 'album_art_url': album_art_url,
  }

def insert_into_db(data, cursor):
  ''' Insert data into a sqlite3 database '''
  for review in data:
    artist = review.get('artist')
    album = review.get('album')
    year_released = review.get('year_released')
    rating = review.get('rating')
    small_text = review.get('small_text')
    review_text = review.get('review')  # 'review' is a reserved word in Python
    reviewer = review.get('reviewer')
    genre = review.get('genre')
    label = review.get('label')
    reviewed = review.get('reviewed')
    album_art_url = review.get('album_art_url')

    cursor.execute('SELECT * FROM reviews WHERE artist=? AND album=?', (artist, album))
    result = cursor.fetchone()
    if result is None:
      # Insert new review into database
      cursor.execute('''
        INSERT INTO reviews (
          artist, album, year_released, rating, small_text,
          review, reviewer, genre, label, reviewed, album_art_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ''', (
        artist, album, year_released, rating, small_text, review_text,
        reviewer, genre, label, reviewed, album_art_url
      ))

def main(start, end):
  conn = sqlite3.connect('reviews.db')
  c = conn.cursor()

  # Create table with all necessary fields
  c.execute('''
    CREATE TABLE IF NOT EXISTS reviews (
      artist TEXT,
      album TEXT,
      year_released INTEGER,
      rating REAL,
      small_text TEXT,
      review TEXT,
      reviewer TEXT,
      genre TEXT,
      label TEXT,
      reviewed TEXT,
      album_art_url TEXT
    )
  ''')

  # Read URLs from a CSV file into a list
  df = pd.read_csv('urls.csv')
  urls = df['url'].tolist()  # replace 'url' with your actual column name
  start, end = max(0, start), min(len(urls), end)
  urls = urls[start:end]

  print(f'Fetching {len(urls)} reviews')
  data = get_reviews(urls)
  print(f'Fetching complete. Inserting into database')
  insert_into_db(data, c)
  print('Done')

  conn.commit()
  conn.close()

if __name__ == '__main__':
  main(int(argv[1]), int(argv[2]))
