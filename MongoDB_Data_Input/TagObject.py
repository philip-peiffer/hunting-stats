# This file defines the object for the tag level queries/stats
import pymongo
import TagPointStat
import TagYearStat

class TagObject:

    def __init__(self, tag_num: str, doc_collection: pymongo.collection.Collection, species: str, start_year: int,
                 end_year: int, residency: str, simple_search=False):
        self.tag = tag_num
        self.doc_coll = doc_collection
        self.species = species.upper()
        self.start = start_year
        self.end = end_year
        self.residency = residency.upper()
        self.year_stats = []
        self.point_stats = []
        
        self.exists = self.simple_search()

        if not simple_search:
            self.query_year_stats()
            self.query_point_stats()
        

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
            new_yr_obj = TagYearStat.YearStat(stat['_id']['year'])
            
            new_yr_obj.set_apps(stat['sum_apps'])
            new_yr_obj.set_successes(stat['sum_tags'])
            new_yr_obj.set_pts_spent(stat['sum_pts'])
            new_yr_obj.set_perc_success()
            new_yr_obj.set_avg_pts_per_app(stat['sum_wa_pts'])
            
            self.year_stats.append(new_yr_obj)

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
            new_pts_obj = TagPointStat.PointStat(self.end, stat['_id']['points'])
            new_pts_obj.set_apps(stat['sum_apps'])
            new_pts_obj.set_successes(stat['sum_tags'])
            new_pts_obj.set_perc_success()
            self.point_stats.append(new_pts_obj)

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
