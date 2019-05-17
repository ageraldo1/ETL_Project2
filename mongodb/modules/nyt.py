import requests
import time
import json
import pymongo
from pprint import pprint
from pymongo import MongoClient

#begin,end dates should be in format "YYYY-MM-DD"
def run(mongo_url, reload, apikey, begin, end):
    db_name = 'TheDonald'
    db_collection = 'nyt'
    time_to_sleep = 10

    hits = 1
    page = 1

    client = MongoClient(mongo_url)
    db = client[db_name]

    if reload == True:
        db.drop_collection(db_collection)    

    collection = db.get_collection(db_collection)

    while hits >0:
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

            if len(shortlist) == 0:
                #no more records
                break
        
            for thing in shortlist:
                articledict = {}
                articledict["id"]=thing['_id']
                
                if "abstract" in thing: articledict["abstract"]=thing["abstract"]
                if "byline" in thing: articledict["byline"]=thing["byline"]
                if "document_type" in thing: articledict["document_type"]=thing["document_type"]
                if "headline" in thing: articledict["headline"]=thing["headline"]
                if "keywords" in thing: articledict["keywords"]=thing["keywords"]
                if "lead_paragraph" in thing: articledict["lead_paragraph"]=thing["lead_paragraph"]
                if "news_desk" in thing: articledict["news_desk"]=thing["news_desk"]
                if "pub_date" in thing: articledict["pub_date"]=thing["pub_date"]
                if "section_name" in thing: articledict["section_name"]=thing["section_name"]
                if "snippet" in thing: articledict["snippet"]=thing["snippet"]
                if "source" in thing: articledict["source"]=thing["source"]
                if "type_of_material" in thing: articledict["type_of_material"]=thing["type_of_material"]
                if "uri" in thing: articledict["uri"]=thing["uri"]
                if "web_url" in thing: articledict["web_url"]=thing["web_url"]
                if "word_count" in thing: articledict["word_count"]=thing["word_count"]

                collection.insert_one(articledict)

            time.sleep(time_to_sleep)

        else:
            return f'Error loading api {response.status_code}, - details {response.text}'

    return 'Process completed without issue'        





