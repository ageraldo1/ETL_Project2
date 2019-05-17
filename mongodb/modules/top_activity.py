from pymongo import MongoClient

def run(mongo_url):
    db_name = 'TheDonald'
    db_collection = 'tweets'

    client = MongoClient(mongo_url)
    db = client[db_name]

    collection = db.get_collection(db_collection)

    top_activity = [
        {
            '$group': {
                '_id' : {
                    'date' : {'$dateToString' : {'format' : '%Y-%m-%d', 'date' : '$timestamp' }}
                },
                'count' : {
                    '$sum' : 1
                }
            }    
        },
        {
            '$sort' : { 'count' : -1}
        },
        {
            '$limit' : 1
        }
    ]

    return [record['_id']['date'] for record in collection.aggregate(top_activity)]


