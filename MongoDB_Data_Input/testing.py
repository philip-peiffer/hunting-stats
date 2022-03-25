from pymongo import MongoClient
from pprint import pprint

# connect to MongoDB running on localhost, port 27017 (defaults)
client = MongoClient()

# connect to a specific database
db = client.hunting_db

serverStatusResult = db.command("serverStatus")
pprint(serverStatusResult)
