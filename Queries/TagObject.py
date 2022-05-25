# This file defines the object for the tag level queries/stats
import pymongo
import PointStat
import YearStat
import requests

class TagObject:

    def __init__(self, tag_num: str, doc_collection: pymongo.collection.Collection, species: str, start_year: int,
                 end_year: int, residency: str, simple_search=False):
        self.tag = tag_num
        self.doc_coll = doc_collection
        self.species = species.upper()
        self.start = start_year
        self.end = end_year
        self.residency = residency.upper()
        self.year_stats = [YearStat.YearStat(year) for year in range(self.start, self.end+1)]
        self.point_stats = [PointStat.PointStat(self.end, point) for point in range(21)]
        
        self.exists = self.simple_search()

        if not simple_search:
            self.query_year_stats()
            self.query_point_stats()
            self.predict_applicants()
        

    def simple_search(self):
        """A simple search that returns "true" if the tag is found in the database, otherwise returns false"""
        pipeline = [
        # match the species, residency, and tag number (tag numbers are shared amongst species so need to match
        # species as well)
            {
                '$match': {'species': self.species, 'tag_num': self.tag, 'residency': self.residency,
                            'dwg_year': {'$gte': self.start, '$lte': self.end}
                            }
            },
        ]
        
        results = self.doc_coll.aggregate(pipeline)
        for result in results:
            return True
        return False

    def query_year_stats(self):
        """Returns a list of total applicants, number of successes, and a weighted average pts/app by year with index
        0 being the start year."""

        pipeline = [
            # match the species, residency, and tag number (tag numbers are shared amongst species so need to match
            # species as well)
            {
                '$match': {'species': self.species, 'tag_num': self.tag, 'residency': self.residency,
                           'dwg_year': {'$gte': self.start, '$lte': self.end}
                           }
            },
            # add a new field in the documents for wa_points, which will be used to find the weighted average
            # of the applicants (finding pts/application = SUM(pt_val*#applicants) / SUM(#applicants)
            {
                '$set': {'wa_points': {'$multiply': ['$applicants', '$point_val']}}
            },
            # group by year and calculate stats
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

        stats = self.doc_coll.aggregate(pipeline)

        # now loop through mongo query and create a year stat object for each stat, add it to the list of year stats
        for stat in stats:
            yr_stat_obj = self.year_stats[stat['_id']['year'] - self.start]
            
            yr_stat_obj.set_apps(stat['sum_apps'])
            yr_stat_obj.set_successes(stat['sum_tags'])
            yr_stat_obj.set_pts_spent(stat['sum_pts'])
            yr_stat_obj.set_perc_success()
            yr_stat_obj.set_avg_pts_per_app(stat['sum_wa_pts'])

    def query_point_stats(self):
        """Queries all the points stats desired. Converts the output to a point stat object and adds point stat object
        to list of point stats."""

        pipeline = [
            {
                '$match': {'species': self.species, 'tag_num': self.tag, 'residency': self.residency,
                           'dwg_year': {'$eq': self.end}
                           }
            },
            {
                '$group': {'_id': {'points': '$point_val'},
                           'sum_apps': {'$sum': '$applicants'},
                           'sum_tags': {'$sum': '$successes'},
                           'sum_pts': {'$sum': '$total_points'},
                           }
            },
            {
                '$sort': {'_id': pymongo.ASCENDING}
            }
        ]

        pt_stats = self.doc_coll.aggregate(pipeline)

        # now loop through pt_stats and create a new object for each result, add to point_stat list
        for stat in pt_stats:
            pts_stat_obj = self.point_stats[stat['_id']['points']]
            pts_stat_obj.set_apps(stat['sum_apps'])
            pts_stat_obj.set_successes(stat['sum_tags'])
            pts_stat_obj.set_perc_success()

    def predict_applicants(self):
        """Calls a microservice with requests API to determine applicants for next year"""
        last_years_apps = [stat.get_applicants() for stat in self.point_stats]
        last_years_successes = [stat.get_successes() for stat in self.point_stats]
        request_data = {
            'prevYearApplication': last_years_apps, 
            'prevYearSuccess': last_years_successes
        }

        r = requests.get('http://localhost:58585/calculate_odds', json=request_data)
        resp_body = r.json()
        
        try:
            next_years_apps = resp_body['calculated']
        except KeyError:
            next_years_apps = [0] * 21
        
        for i, point_stat in enumerate(self.point_stats):
            point_stat.set_next_years_apps(next_years_apps[i])

    def get_stats_dict_format(self, stat_list):
        new_list = [0] * len(stat_list)
        for i, stat in enumerate(stat_list):
            new_list[i] = stat.convert_to_dict()
        return new_list

    def convert_to_dict(self):
        return {
            "tag": self.tag,
            "species": self.species,
            "years": [num for num in range(self.start, self.end + 1)],
            "residency": self.residency,
            "year stats": self.get_stats_dict_format(self.year_stats),
            "point stats": self.get_stats_dict_format(self.point_stats),
        }
