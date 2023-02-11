import logging
import os
from dotenv import load_dotenv
import pymongo

class HuntingDatabase:
    load_dotenv()
    MONGO_URI = os.getenv("MONGODB_URI")

    def __init__(self) -> None:
        self.log = logging.getLogger("HuntingDB")
        self._client = pymongo.MongoClient(self.MONGO_URI)
        self._db = self._client.hunting_research
        self._collection = None

    def test_connection(self):
        return [self._db.list_collection_names()]

    def close_connection(self):
        self._client.close()

    def set_collection(self, coll):
        self._collection = self._db.get_collection(coll)

    def get_collection(self, coll):
        return self._collection

    def clear_collection(self):
        if self._collection is None:
            raise NotImplementedError('Collection has not been set yet')
        
        self._collection.drop()