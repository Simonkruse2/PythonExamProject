from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import requests
import pandas as pd
import numpy as np
import re
import time


def budgetscaper(movies):
    links = []
    listBudget = []
    listLink = []
    numberOfMovies = len(movies)
    # Returns the page
    def fetch_res(url):
        return requests.get(
            url, headers={"Accept-Language": "en-US, en;q=0.5", "Accept-Encoding": "utf-8"}
        )

    for i in range(len(movies)):
        listLink.append(movies.link[i])
        links.append(str("https://www.imdb.com" + movies.link[i] + "?"))

    start_time = time.time()
    with ThreadPoolExecutor(max_workers=5) as executor:
        count = 0
        for link in executor.map(fetch_res, links):
            budget_soup = BeautifulSoup(link.text, "html.parser")
            # time.sleep(0.3)
            txtblock = str(budget_soup.find("div", class_="article", id="titleDetails")).splitlines()
            matchend = re.compile(r'<h4 class="inline">Budget:')
            budget = None
            for ele in txtblock:
                if matchend.findall(ele):
                    dollar = ele.split("</h4>")[1]
                    budget = dollar.replace(",", "")
                    listBudget.append(budget)
                    break
            if(budget is None):
                print(listLink[count], "---- REMOVED ----")
                listLink.pop(count)
                try:
                    movies.drop(count, inplace=True, axis=0)
                except:
                    print("Failed to remove movie, will be handled at mergestep")
            else:
                count = count + 1
                print(count, ' Out of: ', len(listLink), "/", numberOfMovies)
                

    budget_dict = zip(listBudget,listLink)
    columns = ["budget", "link"]
    df_budget = pd.DataFrame(budget_dict, columns=columns)
    df_budget.to_csv('budgets.csv', index=False, header=True)
    movies_final = movies.merge(df_budget, on='link', how='left')
    movies_final['budget'].replace('', np.nan, inplace=True)
    movies_final.dropna(subset=['budget'], inplace=True)
    movies_final.reset_index(drop=True, inplace=True)

    print(df_budget)
    print(movies)
    print(movies_final)

    movies_final.to_csv('movie_ratings_final.csv')

    print(time.time() - start_time, "seconds")
