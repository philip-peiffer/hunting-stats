import pymongo

# connect to the collection that we would like to query
connection = pymongo.MongoClient()
db = connection.hunting_research
collection = db.drawing_results

def load_non_residents():
    pipeline = [
        {
            '$match': {'tag_num': '410-20'}
        }
    ]

    results = collection.aggregate(pipeline)

    print(list(results))

load_non_residents()