# This file defines the object for the region level queries/stats
import pymongo
import YearStat

num_years = 5

class RegionsObject:

    def __init__(self, residency: str, species: str, doc_collection: pymongo.collection.Collection,
                 end_year: int, region: int, query_data=True):
        self.residency = residency.upper()
        self.species = species.upper()
        self.doc_coll = doc_collection
        self.region = region
        self.start = end_year - num_years + 1
        self.end = end_year
        self.years = [num for num in range(self.start, self.end + 1)]
        self.year_stats = None

        if query_data:
            self.year_stats = [YearStat.YearStat(year) for year in self.years]
            self.query_region_data()

    def query_region_data(self):
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
                '$group': {'_id': {'year': '$dwg_year'},
                           'sum_apps': {'$sum': '$applicants'},
                           'sum_tags': {'$sum': '$successes'},
                           'sum_pts': {'$sum': '$total_points'},
                           'sum_wa_pts': {'$sum': '$wa_points'}
                           }
            },
            {
                '$sort': {'_id': pymongo.ASCENDING}
            }
        ]

        results = self.doc_coll.aggregate(pipeline)

        # loop through results, assigning values to YearStat objects
        for stat in results:
            yr_stat_obj = self.year_stats[stat['_id']['year'] - self.start]
            
            yr_stat_obj.set_apps(stat['sum_apps'])
            yr_stat_obj.set_successes(stat['sum_tags'])
            yr_stat_obj.set_pts_spent(stat['sum_pts'])
            yr_stat_obj.set_perc_success()
            yr_stat_obj.set_avg_pts_per_app(stat['sum_wa_pts']) 

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

    def get_stats_dict_format(self, stat_list):
            new_list = [0] * len(stat_list)
            for i, stat in enumerate(stat_list):
                new_list[i] = stat.convert_to_dict()
            return new_list

    def convert_to_dict(self):
        return {
            "region": self.region,
            "species": self.species,
            "years": [num for num in range(self.start, self.end + 1)],
            "residency": self.residency,
            "year stats": self.get_stats_dict_format(self.year_stats),
        }
