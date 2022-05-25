# This file defines the object for the region level queries/stats
import pymongo

num_years = 5

class RegionsObject:

    def __init__(self, residency: str, species: str, doc_collection: pymongo.collection.Collection,
                 end_year: int, region: int, query_data: True):
        self.residency = residency.upper()
        self.species = species.upper()
        self.doc_coll = doc_collection
        self.region = region
        self.start = end_year - num_years + 1
        self.end = end_year
        self.years = [num for num in range(self.start, self.end + 1)]
        self.data = []

    def set_data(self):
        """Sets self.data with data returned by pymongo. The query filters on residency, species,
        and years. It then groups by region to get aggregated stats"""

        pipeline = [
            {
                '$match': {'residency': self.residency, 'species': self.species,
                           'dwg_year': {'$gte': self.start, '$lte': self.end}}
            },
            {
                '$set': {'wa_points': {'$multiply': ['$applicants', '$point_val']}}
            },
            {
                '$group': {'_id': {'region': '$region', 'year': '$dwg_year'},
                           'num apps': {'$sum': '$applicants'},
                           'num tags': {'$sum': '$successes'},
                           'pts spent': {'$sum': '$total_points'},
                           'wgt_avg_helper': {'$sum': '$wa_points'}
                           }
            },
            {
                '$sort': {'_id': pymongo.ASCENDING}
            }
        ]

        results = self.doc_coll.aggregate(pipeline)

        # now loop through results and assign each batch to data
        new_obj = {
            'region': None,
            'num apps': [],
            'num tags': [],
            'pts spent': [],
            'avg pts per app': [],
            'years': self.years
        }

        for result in results:
            year_ind = result['_id']['year'] - self.start

            if result['_id']['region'] != new_obj['region']:
                if new_obj['region'] is not None:
                    self.data.append(new_obj)
                new_obj = {
                    'region': result['_id']['region'],
                    'num apps': [0] * num_years,
                    'num tags': [0] * num_years,
                    'pts spent': [0] * num_years,
                    'avg pts per app': [0] * num_years,
                    'years': self.years
                }

            new_obj['num apps'][year_ind] = result['num apps']
            new_obj['num tags'][year_ind] = result['num tags']
            new_obj['pts spent'][year_ind] = result['pts spent']
            new_obj['avg pts per app'][year_ind] = round(result['wgt_avg_helper'] / result['num apps'], 1)

        self.data.append(new_obj)

    def get_districts(self):
        """Gets the districts for the given region"""
        pipeline = [
            {
                '$match': {'residency': self.residency, 'species': self.species, 'region': self.region,
                           'dwg_year': {'$gte': self.start, '$lte': self.end}}
            },
            {
                '$group': {'_id': {'district': '$district'}}
            },
            {
                '$sort': {'_id': pymongo.ASCENDING}
            }
        ]
        results = self.doc_coll.aggregate(pipeline)
        return list(results)
