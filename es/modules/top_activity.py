from elasticsearch import Elasticsearch

def run(es_url):
    es = Elasticsearch(hosts=es_url)

    index_name = 'tweets'

    top_activity = {        
        "size": 0, 
        "aggs": {
            "group_by_tweets": {
                "date_histogram": {
                    "field": "timestamp",
                    "interval": "day"
                },
            "aggs": {
                "total_tweets": {
                    "value_count": {"field": "timestamp"}
                },
                "sales_bucket_sort" : {
                    "bucket_sort": {
                        "sort": [{"total_tweets" : {"order": "desc"} }],
                        "size": 1
                    }                
                }
            }
            }
        }
    }

    res = es.search(index=index_name, body=top_activity)
    
    return [record['key_as_string'] for record in res['aggregations']['group_by_tweets']['buckets']]


