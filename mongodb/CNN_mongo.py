import requests
import time
import pymongo
import pprint

apikey = "8ddbfb9823944cce86f708d73414889d"



begin = "2015-07-16"
end = "2016-11-11"
query = (f"https://newsapi.org/v2/everything?sources=cnn&from={begin}&to={end}&apiKey={apikey}")

articles = requests.get(query).json()
articlelist = [article for article in articles['articles']]

conn = 'mongodb://localhost:27017'
client = pymongo.MongoClient(conn)

db = client.TheDonald

collection = db.CNN

collection.remove({})

for article in articlelist:
    collection.insert_one(article)

results = collection.find()
for result in results:
    print(result)