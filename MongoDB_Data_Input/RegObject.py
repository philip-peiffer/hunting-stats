# This file defines the object for the region level queries/stats
import pymongo


class RegObject:

    def __init__(self, region: str, doc_collection: pymongo.collection.Collection):
        self.region = region
        self.doc_coll = doc_collection
        self.districts = dict()

    def set_districts(self):
        """Sets self.districts as a list of the districts in the doc_collection from mongoDB that reside
        within the region."""

        pipeline = [
            {
                '$match': {'region': self.region}
            },
            {
                '$group': {'_id': {'species': '$species', 'district': '$district'}}
            },
            {
                '$sort': {'_id': pymongo.ASCENDING}
            }
        ]

        results = self.doc_coll.aggregate(pipeline)

        for result in results:
            try:
                self.districts[result['_id']['species']].append(result['_id']['district'])
            except KeyError:
                self.districts[result['_id']['species']] = []
                self.districts[result['_id']['species']].append(result['_id']['district'])

    def print_districts(self, species=None):
        """Prints the dictionary of districts"""
        if species is not None:
            print('------- {} Districts -------'.format(species))
            for dist in self.districts[species.upper()]:
                print("{},".format(dist), end=" ")
            print("\n")

        else:
            for species in self.districts:
                print('------- {} Districts -------'.format(species))
                for dist in self.districts[species]:
                    print("{},".format(dist), end=" ")
                print("\n")
