# This file defines the object for the district level queries/stats
import pymongo

num_years = 5

class DistObject:

    def __init__(self, doc_collection: pymongo.collection.Collection, species: str, residency: str, region: str,
                 end_year: int):
        self.start = end_year - num_years + 1
        self.end = end_year
        self.years = [num for num in range(self.start, self.end + 1)]
        self.region = region
        self.doc_coll = doc_collection
        self.species = species.upper()
        self.residency = residency.upper()
        self.data = []
        # format for data array will be array of dictionaries as follows:
        # self.data = {district: XXX,
        #              years: XXX,
        #              district_data: {
        #                               num apps: [],
        #                               num tags: [], 
        #                               pts spent: [],
        #                               avg pts per app: []
        #                              }
        #            }
    
    def query_districts_data(self):
        """This function fetches the data for the districts within the region defined in the properties above and formats it to fit
        within the self.data list."""
        pipeline = [
            {
                '$match': {'residency': self.residency, 'species': self.species, 'region': self.region,
                           'dwg_year': {'$gte': self.start, '$lte': self.end}}
            },
            {
                '$set': {'wa_points': {'$multiply': ['$applicants', '$point_val']}}
            },
            {
                '$group': {'_id': {'district': '$district', 'year': '$dwg_year'},
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

        # loop through results to get into correct dictionary format
        curr_dist = None
        for result in results:
            if curr_dist is None or result['_id']['district'] != curr_dist['district']:
                if curr_dist is not None:
                    self.data.append(curr_dist)
                
                curr_dist = {
                    'district': result['_id']['district'],
                    'years': self.years,
                    'district data': {
                        'num apps': [0] * num_years,
                        'num tags': [0] * num_years,
                        'pts spent': [0] * num_years,
                        'avg pts per app': [0] * num_years
                    }
                }
            
            year_ind = result['_id']['year'] - self.start
            avg_pts = round(result['wgt_avg_helper'] / result['num apps'], 1)

            curr_dist['district data']['num apps'][year_ind] = result['num apps']
            curr_dist['district data']['num tags'][year_ind] = result['num tags']
            curr_dist['district data']['pts spent'][year_ind] = result['pts spent']
            curr_dist['district data']['avg pts per app'][year_ind] = avg_pts
        
        self.data.append(curr_dist)

    def get_tags(self, district: str):
        """This function gets the tags within the district passed in as an argument"""
        pipeline = [
            {
                '$match': {'residency': self.residency, 'species': self.species, 'region': self.region, 'district': district,
                           'dwg_year': {'$gte': self.start, '$lte': self.end}}
            },
            {
                '$group': {'_id': {'tag num': '$tag_num'}}
            },
            {
                '$sort': {'_id': pymongo.ASCENDING}
            }
        ]
        results = self.doc_coll.aggregate(pipeline)
        return list(results)

