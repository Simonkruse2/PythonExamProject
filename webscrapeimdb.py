import requests
from bs4 import BeautifulSoup
import pandas as pd
from IPython.core.display import clear_output
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from random import randint
from time import time
import budgetscraping
import re

listTitles = []
listYears = []
listAgeRestrictions = []
listRunTimes = []
listImdbScores = []
listMetaScores = []
listGenres = []
listDirectors = []
listActors = []
listGross = []
listVotes = []
listLinks = []
links = []
newurl = 1
def fetch_res(url):
    return requests.get(
        url, headers={"Accept-Language": "en-US, en;q=0.5", "Accept-Encoding": "utf-8"}
    )
for i in range(40):
    links.append("https://www.imdb.com/search/title/?title_type=feature&release_date=2000-01-01,2020-05-05&languages=en&sort=boxoffice_gross_us,desc&count=250&start="
        + str(newurl))
    newurl = newurl + 250

# Get all 250 movies on a single page out of 40 pages
with ThreadPoolExecutor(max_workers=5) as executor:
    count = 0 
    for link in executor.map(fetch_res, links):
        start_time = time()
        html_soup = BeautifulSoup(link.text, "html.parser")
        movies = html_soup.find_all("div", class_="lister-item mode-advanced")
        movies_before_pd = []
        for i in range(250):
            directors = []
            actors = []
            genres = []
            # Get metascore rating
            if (
                movies[i].find("div", class_="ratings-metascore") is not None
                and movies[i].find("span", class_="certificate") is not None
                and movies[i]
                .find("p", class_="sort-num_votes-visible")
                .find("span", class_="ghost")
                is not None
            ):

                # Get first movies title
                link = movies[i].find("h3", class_="lister-item-header").a["href"]
                # print(link)
                title = movies[i].h3.a.text
                # Get release year
                year = (
                    movies[i]
                    .find("span", class_="lister-item-year text-muted unbold")
                    .text.replace("(", "")
                    .replace(")", "")
                )

                # Get ageRestriction - AKA Parental guidance
                ageRestriction = movies[i].find("span", class_="certificate").text

                # Get runtime
                runtime = movies[i].find("span", class_="runtime").text
                # Get IMDB rating - multiply by 10, so we're able to compare it to metascore score, which is on a 0-100 scale
                imdbRating = (
                    float(
                        movies[i]
                        .find("div", class_="inline-block ratings-imdb-rating")
                        .strong.text
                    )
                    * 10
                )
                metascore = float(movies[i].find("span", class_="metascore").text.strip())
                # Get all genres - dont forGet to remove whitespace and do commaseparation
                genres = movies[i].find("span", class_="genre").text.strip().split(", ")
                # Get actor and director ("adString")
                adString = str(movies[i].find("p", class_=""))
                ad = adString.splitlines()
                match_end = re.compile(r"</a>$")
                match_end2 = re.compile(r"</a>, $")
                index = 0

                # Get directors
                for idx, director in enumerate(ad):

                    if director == "    Stars:":
                        index = idx
                        break
                    if match_end.findall(director) or match_end2.findall(director):
                        director1 = director.split("</a>")
                        director2 = director1[0].split('/">')
                        directors.append(director2[1])

                # Get actors
                for actor in ad[index:]:
                    if actor == "</p>":
                        break
                    if match_end.findall(actor) or match_end2.findall(actor):
                        actor1 = actor.split("</a>")
                        actor2 = actor1[0].split('/">')
                        actors.append(actor2[1])
                # Get votes from a single movie
                votes = movies[i].find_all("span", attrs={"name": "nv"})[0]["data-value"]
                # Get gross profit from a single movie
                gross = (
                    movies[i]
                    .find_all("span", attrs={"name": "nv"})[1]["data-value"]
                    .replace(",", "")
                )
          
                listLinks.append(link)
                listTitles.append(title)
                listYears.append(year)
                listAgeRestrictions.append(ageRestriction)
                listRunTimes.append(runtime)
                listImdbScores.append(imdbRating)
                listMetaScores.append(metascore)
                listGenres.append(genres)
                listDirectors.append(directors)
                listActors.append(actors)
                listGross.append(gross)
                listVotes.append(votes)
        current_time = time()
        elapsed_time = current_time - start_time
        count = count + 1
        print(count, ' Out of ', len(links), ' total pages of movies, containing 250 movies')


movies_dict = {
    "title": listTitles,
    "year": listYears,
    "ageRestriction": listAgeRestrictions,
    "runtime": listRunTimes,
    "imdbScore": listImdbScores,
    "metaScore": listMetaScores,
    "genres": listGenres,
    "directors": listDirectors,
    "actors": listActors,
    "gross": listGross,
    "votes": listVotes,
    "link": listLinks,
}

columns = [
    "title",
    "year",
    "ageRestriction",
    "runtime",
    "imdbScore",
    "metaScore",
    "genres",
    "directors",
    "actors",
    "gross",
    "votes",
    "link",
]
df_final = pd.DataFrame(movies_dict, columns=columns)
df_final.to_csv("movie_ratings.csv")
print(df_final)

budgetscraping.budgetscaper(df_final)