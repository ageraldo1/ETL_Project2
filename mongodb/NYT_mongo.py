import requests
import time
import json
import pymongo
from pprint import pprint

articlelist = []
for x in range(1,965,1):
    page = x
    apikey = "z0BkjWA7vgZ1bdYI9RUOZP8S3uubUZ4p"
    params = {
        'api-key' : apikey,
        'begin_date' : '20151001',
        'end_date' : '20151031',
        'sort':'newest',
        'page':1
            }
    url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'

    response = requests.get(url=url,params=params)

    if response.status_code == 200:
        articles= response.json()
        shortlist = [article for article in articles["response"]["docs"]]
    for thing in shortlist:
        articlelist.append(thing)
conn = 'mongodb://localhost:27017'
client = pymongo.MongoClient(conn)

db = client.TheDonald

collection = db.NYT

collection.remove({})

for article in articlelist:
    collection.insert_one(article)

results = collection.find()
for result in results:
    print(result)

