# This file defines the object for the district level queries/stats
import pymongo
import YearStat

num_years = 5

class DistObject:

    def __init__(self, doc_collection: pymongo.collection.Collection, species: str, residency: str, district: str,
                 end_year: int, query_data=True):
        self.start = end_year - num_years + 1
        self.end = end_year
        self.years = [num for num in range(self.start, self.end + 1)]
        self.district = district
        self.doc_coll = doc_collection
        self.species = species.upper()
        self.residency = residency.upper()
        self.year_stats = None

        if query_data:
            self.year_stats = [YearStat.YearStat(year) for year in self.years]
            self.query_district_data()
        
    
    def query_district_data(self):
        """This function fetches the data for the districts within the region defined in the properties above and formats it to fit
        within the self.data list."""
        pipeline = [
            {
                '$match': {'residency': self.residency, 'species': self.species, 'district': self.district,
                           'dwg_year': {'$gte': self.start, '$lte': self.end}}
            },
            {
                '$set': {'wa_points': {'$multiply': ['$applicants', '$point_val']}}
            },
            {
                '$group': {'_id': {'year': '$dwg_year'},
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

        # loop through results, assigning values to YearStat objects
        for stat in results:
            yr_stat_obj = self.year_stats[stat['_id']['year'] - self.start]
            
            yr_stat_obj.set_apps(stat['sum_apps'])
            yr_stat_obj.set_successes(stat['sum_tags'])
            yr_stat_obj.set_pts_spent(stat['sum_pts'])
            yr_stat_obj.set_perc_success()
            yr_stat_obj.set_avg_pts_per_app(stat['sum_wa_pts'])       

    def get_tags(self):
        """This function gets the tags within the district passed in as an argument"""
        pipeline = [
            {
                '$match': {'residency': self.residency, 'species': self.species, 'region': self.region, 'district': self.district,
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

    def get_stats_dict_format(self, stat_list):
            new_list = [0] * len(stat_list)
            for i, stat in enumerate(stat_list):
                new_list[i] = stat.convert_to_dict()
            return new_list

    def convert_to_dict(self):
        return {
            "district": self.tag,
            "species": self.species,
            "years": [num for num in range(self.start, self.end + 1)],
            "residency": self.residency,
            "year stats": self.get_stats_dict_format(self.year_stats),
        }
