from requests import get
from bs4 import BeautifulSoup
import pandas as pd
from IPython.core.display import clear_output
from time import sleep
from random import randint
from time import time

# Check type of the requested resource
# print(type(movies))
# Check how many resources were gathered
# print(len(movie))
allmovies = []
newurl = 1

# A request would go here
# Get all 250 movies on a single page out of 296 pages (all movies from 2000-2020)
for i in range(296): 
    start_time = time()
    requests = i
    requests += 1
    sleep(1)
    current_time = time()
    elapsed_time = current_time - start_time
    url = 'https://www.imdb.com/search/title/?title_type=feature&release_date=2000-01-01,2020-05-05&languages=en&sort=boxoffice_gross_us,desc&count=250&start='+str(newurl)
    newurl = newurl + 250
    # Dont forGet to add headers US, otherwise imdb data will be DK geobased
    response = get(url,headers = {"Accept-Language": "en-US, en;q=0.5","Accept-Encoding": "utf-8"})
    # Check if the request was successful
    # Print(response.text[:200]) 
    html_soup = BeautifulSoup(response.text, 'html.parser')
    type(html_soup)

    # Get all movies from imdb from year 2000 to present 
    movies = html_soup.find_all('div', class_ = 'lister-item mode-advanced')
    movies_before_pd=[]
    for i in range(250):
        directors=[]
        actors=[]
        genres=[]
        # Get metascore rating
        if movies[i].find('div', class_ = 'ratings-metascore') is not None:
            movies[i].encode('utf-8')   
            # Get first movies title
            title = movies[i].h3.a.text

            # Get release year
            year = movies[i].find('span', class_ = 'lister-item-year text-muted unbold').text.replace('(','').replace(')','')

            # Get ageRestriction - AKA Parental guidance 
            ageRestriction = movies[i].find('span', class_ = 'certificate').text

            # Get runtime
            runtime = movies[i].find('span', class_ = 'runtime').text

            # Get IMDB rating - multiply by 10, so we're able to compare it to metascore score, which is on a 0-100 scale
            imdbRating = float(movies[i].find('div', class_ = 'inline-block ratings-imdb-rating').strong.text)*10
            metascore = float(movies[i].find('span', class_ = 'metascore').text.strip())
            # Get all genres - dont forGet to remove whitespace and do commaseparation
            genres = movies[i].find('span', class_ = 'genre').text.strip().split(', ')
            # Get director
            director = movies[i].find('p',class_='').find_all('a')[0].text
            directors.append(director)
            # Get actors
            actors = [a.text for a in movies[i].find('p',class_='').find_all('a')[1:]]
            # Get votes from a single movie
            votes = movies[i].find_all('span', attrs = {'name':'nv'})[0]['data-value']
            # Get gross profit from a single movie
            gross = movies[i].find_all('span', attrs = {'name':'nv'})[1]['data-value'].replace(',', '')
            movie = {
                'title': title,
                'year': year,
                'ageRestriction': ageRestriction,
                'runtime': runtime,
                'imdbRating': imdbRating,
                'metascore': metascore,
                'genres': genres,
                'directors': directors,
                'actors': actors,
            }
            movies_before_pd.append(movie)
    print('Request: {}; Frequency: {} requests/s'.format(requests, requests/elapsed_time))
    allmovies.append(movies_before_pd)
    print(url)
    print(newurl)
finished_movies = pd.DataFrame(allmovies)
finished_movies.to_csv('movie_ratings.csv')
print(allmovies)
# clear_output(wait = True)
# print(finished_movies)