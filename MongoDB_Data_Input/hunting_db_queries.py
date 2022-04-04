import sys

import pymongo
from dotenv import load_dotenv
import os
from pprint import pprint

# load config from the .env file
load_dotenv(verbose=True)
MONGODB_URI = os.environ["MONGODB_URI"]

# connect to the collection that we would like to query
connection = pymongo.MongoClient()
db = connection.hunting_research
collection = db.drawing_results

class StatQueryResults:
    """Defines the format for query results"""

    def __init__(self, tag_num, title, start_year, end_year):
        self.start_year = start_year
        self.end_year = end_year
        self.tag_num = tag_num
        self.stats = {title: ['' for num in range(start_year, end_year+1)]}

    def add_new_stat(self, stat_title):
        self.stats[stat_title] = ['' for num in range(self.start_year, self.end_year+1)]

    def print_stat(self, stat_title, print_header):
        # print the header
        if print_header:
            print('------- {} ------------'.format(stat_title.upper()))
            for num in range(self.start_year, self.end_year + 1):
                if num == self.start_year:
                    print("|{:^8}".format(''), end='')
                print("|{:^8}".format(num), end='')
            print()

        # print the row of stats
        i = 0
        for stat in self.stats[stat_title]:
            if i == 0:
                print("|{:^8}".format(self.tag_num), end='')
                i += 1
            print("|{:^8}".format(stat), end='')
        print()


class DrawingQueries:
    """Defines the queries used with the drawing results collection"""
    allowed_res_terms = {"RESIDENT", "NON-RESIDENT", "RESIDENT LANDOWNER", "NON-RESIDENT LANDOWNER"}

    def __init__(self, dwg_results_collection, species: str, residency: str):
        self.docs = dwg_results_collection
        self.tag_term = None
        self.region_term = None
        self.point_term = None
        self.year_term = None

        if species.isalpha():
            self.species = species.upper()
        else:
            self.species = None
            raise ValueError

        if residency.isalpha() and residency.upper() in self.allowed_res_terms:
            self.residency = residency.upper()
        else:
            self.residency = None
            raise ValueError

    def clear_search_terms(self):
        """This function clears the search terms from the class attributes"""
        self.tag_term = None
        self.res_term = None
        self.region_term = None
        self.point_term = None
        self.year_term = None
        self.species = None

    def set_tag_term(self):
        """This function is called to get the tag that the user would like to search for. The function also
        calls set_region_term because the region is the first number in the tag. So when this function is called
        there is no need to call set_region_term as well."""
        self.tag_term = input("What tag are you looking for? Enter 0 if not searching for a tag. \n"
                              ">>> ")

        # set the region term based on the tag input
        self.set_region_term()

    def set_region_term(self):
        """This function is called to get the region search term from the user. This function is only used
        when the tag term is not searched as well."""
        self.region_term = input("What region would you like to see?")

    def set_year_term(self):
        """This function is called to get the year search term from the user."""
        self.year_term = input("What year? Enter 0 for all years. Data goes from 2006 - 2021.\n"
                               ">>> ")

    def set_point_term(self):
        """This function is called to get the number of points that the user would like to filter on"""
        pass

    def get_current_tags(self):
        """This function finds tags that were active in the last year - does not guarantee that these
        tags are still active, as FWP changes tag numbers constantly"""
        pipeline = [
            {
                "$match": {"dwg_year": {"$gt": 2020},
                           "species": self.species
                           }
            },
            {
                "$group": {"_id": "$tag_num"}
            },
            {
                "$sort": {"_id": pymongo.ASCENDING}
            }
        ]
        return collection.aggregate(pipeline)

    def get_dist_by_reg(self, region: str):
        """
        This function gets the districts for a region. It returns a list of the districts in sorted order
        from lowest value to highest.
        :param region:
        :return:
        """

        # get the districts for region and species
        dist_pipeline = [
            {
                '$match': {'region': region, 'species': self.species}
            },
            {
                '$group': {'_id': {'district': '$district'}}
            },
            {
                '$sort': {'_id': pymongo.ASCENDING}
            }
        ]

        return list(self.docs.aggregate(dist_pipeline))

    def get_tags_by_district(self, district: str):
        """This function requires the district to search as a string. It returns a list of all the tags in
        that district since 2006. Note that not all tags are still active. These will still be returned,
        as it helps paint the overall picture of the district."""
        # run a query to match the district, group it by tag number
        pipeline = [
            {
                '$match': {'species': self.species, 'district': district, 'residency': self.residency}
            },
            {
                '$group': {'_id': {'tag': '$tag_num'}}
            },
            {
                '$sort': {'_id': pymongo.ASCENDING}
            }
        ]

        tags = self.docs.aggregate(pipeline)
        return_val = []
        for tag in tags:
            return_val.append(tag['_id']['tag'])

        return return_val

    def get_tag_hlstats_by_year(self, tag: str):
        """This function requires the tag to search as a string. It returns high level statistics for each
        year that the tag was in circulation (# applications, # tags awarded, pts/application). This
        return value is a list sorted on year from low to high."""

        pipeline = [
            {
                '$match': {'species': self.species, 'tag_num': tag, 'residency': self.residency}
            },
            # add a new field in the documents for wa_points, which will be used to find the weighted average
            # of the applicants (finding pts/application = SUM(pt_val*#applicants) / SUM(#applicants)
            {
                '$set': {'wa_points': {'$multiply': ['$applicants', '$point_val']}}
            },
            {
                '$group': {'_id': {'year': '$dwg_year', 'tag_num': '$tag_num'},
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

        result = list(self.docs.aggregate(pipeline))

        # loop through each year in the result and calculate pts/applicant and tack this on to each field
        for year in result:
            pts_per_app = round(year['sum_wa_pts'] / year['sum_apps'], 1)
            year['pts_per_app'] = pts_per_app

        return result

    def get_dist_high_lev_stats_by_year(self, dist: str):

        pipeline = [
            {
                '$match': {'species': self.species, 'district': dist, 'residency': self.residency}
            },
            # add a new field in the documents for wa_points, which will be used to find the weighted average
            # of the applicants (finding pts/application = SUM(pt_val*#applicants) / SUM(#applicants)
            {
                '$set': {'wa_points': {'$multiply': ['$applicants', '$point_val']}}
            },
            {
                '$group': {'_id': {'year': '$dwg_year', 'tag_num': '$tag_num'},
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

        results = list(self.docs.aggregate(pipeline))

        # loop through each year in the result and calculate pts/applicant and tack this on to each field
        for year in results:
            pts_per_app = round(year['sum_wa_pts'] / year['sum_apps'], 1)
            year['pts_per_app'] = pts_per_app

        # now reformat result into StatQueryResult objects so that result always comes back the same
        return_dict = dict()
        for result in results:
            tag_num = result['_id']['tag_num']
            tag_year = result['_id']['year']

            if tag_num not in return_dict:
                new_query_result_object = StatQueryResults(tag_num, 'Tags Issued', 2006, 2022)
                return_dict[tag_num] = new_query_result_object
                return_dict[tag_num].add_new_stat('Applications')
                return_dict[tag_num].add_new_stat('Weighted Average Pts per Applicant')

            return_dict[tag_num].stats['Tags Issued'][tag_year - 2006] = result['sum_tags']
            return_dict[tag_num].stats['Applications'][tag_year - 2006] = result['sum_apps']
            return_dict[tag_num].stats['Weighted Average Pts per Applicant'][tag_year - 2006] = result['pts_per_app']

        return return_dict

    def print_stat_table(self, table_list: list, header: bool):
        pass

    def get_tag_llstats_by_year(self, tag: str):

        buckets = [num for num in range(2006, 2022)]

        pipeline = [
            {
                '$match': {'species': self.species, 'tag_num': tag, 'residency': self.residency}
            },
            {
                '$bucket': {
                    'groupBy': '$dwg_year',
                    'boundaries': buckets,
                    'default': 'Other',
                    'output': {
                        'docs': {
                            '$push': {
                                'tag_num': '$tag_num',
                                'point_val': '$point_val',
                                'applicants': '$applicants',
                                'total_points': '$total_points',
                                'successes': '$successes',
                            }
                        }
                    }
                }
            }
        ]

        return list(self.docs.aggregate(pipeline))

    def print_tag_ll_stats(self, tag_list: list, print_header: bool):
        pass


dq = DrawingQueries(collection, 'moose', 'resident')
print('------------------- districts for region --------------------------')
pprint(dq.get_dist_by_reg('1'))
print('------------------- tags for district -----------------------------')
tags = dq.get_tags_by_district('121')
pprint(tags)

stats = dq.get_dist_high_lev_stats_by_year('121')

i = 0
for tag in stats:
    if i == 0:
        stats[tag].print_stat('Applications', True)
        i += 1
    else:
        stats[tag].print_stat('Applications', False)

pprint(stats)

# close the connection
connection.close()
