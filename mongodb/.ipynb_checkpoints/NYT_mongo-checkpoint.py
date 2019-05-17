import requests
import time
import json
import pymongo
from pprint import pprint
#begin,end dates should be in format "YYYY-MM-DD"
def NYT(begin,end):

    articlelist = []
    hits = 1
    page = 1
    while hits >0:
        apikey = "z0BkjWA7vgZ1bdYI9RUOZP8S3uubUZ4p"
        params = {
            'api-key' : apikey,
            'begin_date' : begin,
            'end_date' : end,
            'sort':'newest',
            'page':page
            }
        url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'

        response = requests.get(url=url,params=params)

        if response.status_code == 200:
            articles= response.json()
            hits = articles["response"]["meta"]["hits"]
            page = page +1
            shortlist = [article for article in articles["response"]["docs"]]
        for thing in shortlist:
            articledict = {}
            articledict["id"]=thing['_id']
            articledict["abstract"]=thing["abstract"]
            articledict["byline"]=thing["byline"]
            articledict["document_type"]=thing["document_type"]
            articledict["headline"]=thing["headline"]
            articledict["keywords"]=thing["keywords"]
            articledict["lead_paragraph"]=thing["lead_paragraph"]
            articledict["news_desk"]=thing["news_desk"]
            articledict["pub_date"]=thing["pub_date"]
            articledict["section_name"]=thing["section_name"]
            articledict["snippet"]=thing["snippet"]
            articledict["source"]=thing["source"]
            articledict["type_of_material"]=thing["type_of_material"]
            articledict["uri"]=thing["uri"]
            articledict["web_url"]=thing["web_url"]
            articledict["word_count"]=thing["word_count"]


            articlelist.append(articledict)
    conn = 'mongodb://localhost:27017'
    client = pymongo.MongoClient(conn)

    db = client.TheDonald

    collection = db.NYT

    for article in articlelist:
        collection.insert_one(article)





