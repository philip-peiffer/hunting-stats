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

    def set_res_term(self):
        """This function is called to get the residency search term from the user"""
        user_input = input("Residency? Enter one of the following integers:\n"
                              "1: Resident\n"
                              "2: Non-Resident\n"
                              "3: Both\n"
                              "4: Resident Landowner\n"
                              "5: Non-Resident Landowner\n"
                              "6: Both\n"
                              "7: All\n"
                              ">>> ")

        if int(user_input) < 1 or int(user_input) > 7:
            raise IndexError

        resident_map = {
            1: "RESIDENT",
            2: "NON-RESIDENT",
            4: "RESIDENT LANDOWNER",
            5: "NON-RESIDENT LANDOWNER",
        }

    def set_point_term(self):
        """This function is called to get the number of points that the user would like to filter on"""

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

        return list(self.docs.aggregate(pipeline))

    def get_tag_hlstats_by_year(self, tag: str):
        """This function requires the tag to search as a string. It returns high level statistics for each
        year that the tag was in circulation (# applications, # tags awarded, pts/application). This
        return value is a list sorted on year from low to high."""
        all_tags = self.get_tags_by_district(tag[0:3])
        buckets = [tag['_id']['tag'] for tag in all_tags]

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

        return_val = list(self.docs.aggregate(pipeline))

        # loop through return_val and calculate pts/applicant and tack this on to each field
        for result in return_val:
            pts_per_app = round(result['sum_wa_pts'] / result['sum_apps'], 1)
            result['pts_per_app'] = pts_per_app

        return return_val

    def print_tag_hlstats(self, tag_list: list, print_header: bool):
        # find the min and max year in the list
        min_year = sys.maxsize
        max_year = 0
        for tag in tag_list:
            tag_year = tag['_id']['year']
            if tag_year < min_year:
                min_year = tag_year
            if tag_year > max_year:
                max_year = tag_year

        # loop through tags and create new row for each tag. Index tag results to correct area based on year
        result_dict = dict()
        result_dict['years'] = [num for num in range(min_year, max_year + 1)]
        for tag in tag_list:
            i = tag['_id']['year'] - min_year

            # add the tag stats to the result_dict
            try:
                result_dict[tag['_id']['tag_num']]['sum_apps'][i] = tag['sum_apps']
                result_dict[tag['_id']['tag_num']]['sum_tags'][i] = tag['sum_tags']
                result_dict[tag['_id']['tag_num']]['pts_per_app'][i] = tag['pts_per_app']
            except KeyError:
                result_dict[tag['_id']['tag_num']] = dict()
                result_dict[tag['_id']['tag_num']]['sum_apps'] = ["" for _ in result_dict['years']]
                result_dict[tag['_id']['tag_num']]['sum_tags'] = ["" for _ in result_dict['years']]
                result_dict[tag['_id']['tag_num']]['pts_per_app'] = ["" for _ in result_dict['years']]

                result_dict[tag['_id']['tag_num']]['sum_apps'][i] = tag['sum_apps']
                result_dict[tag['_id']['tag_num']]['sum_tags'][i] = tag['sum_tags']
                result_dict[tag['_id']['tag_num']]['pts_per_app'][i] = tag['pts_per_app']

        # print the header
        print('------------------- HIGH LEVEL STATS FOR TAGS ----------------------')
        print('Values in cells are total # applications, Total # tags, Average pts per applicant')
        for year in result_dict['years']:
            if year == min_year:
                print("|{:^8}".format(" "), end='')
            print("|{:^8}".format(year), end='')
        print()

        # print the table
        for tag_num in result_dict:
            if tag_num == 'years':
                continue

            # print the tag_num first
            print("|{:^8}\n|{:^8}\n|{:^8}".format("",tag_num,""), end='')

            # now print the stats array
            for stats in result_dict[tag_num]['sum_apps']:
                print("|{:^8}".format(str(stats)), end='')
            print()
        print()

        # print the header
        print('Values in cells are total # tags')
        for year in result_dict['years']:
            if year == min_year:
                print("|{:^8}".format(" "), end='')
            print("|{:^8}".format(year), end='')
        print()

        # print the table
        for tag_num in result_dict:
            if tag_num == 'years':
                continue

            # print the tag_num first
            print("|{:^8}".format(tag_num), end='')

            # now print the stats array
            for stats in result_dict[tag_num]['sum_tags']:
                print("|{:^8}".format(str(stats)), end='')
            print()
        print()

        # print the header
        print('Values in cells are average pts per applicant')
        for year in result_dict['years']:
            if year == min_year:
                print("|{:^8}".format(" "), end='')
            print("|{:^8}".format(year), end='')
        print()

        # print the table
        for tag_num in result_dict:
            if tag_num == 'years':
                continue

            # print the tag_num first
            print("|{:^8}".format(tag_num), end='')

            # now print the stats array
            for stats in result_dict[tag_num]['pts_per_app']:
                print("|{:^8}".format(str(stats)), end='')
            print()
        print()

    def get_tag_llstats_by_year(self, tag: str):

        buckets = [2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]

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
        # in this list, every entry in the list is a new year dictionary and the 'docs' entry for that year
        # is a list of documents with applicant points, successes, etc.
        header = ['Year', 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 'Total app', 'Total pts', 'Total tags']

        # print the header
        if print_header:
            print('------------------- LOW LEVEL STATS PT1 FOR TAG {} ----------------------'.format(
                tag_list[0]['docs'][0]['tag_num'])
            )
            print('Values in cells are as follows: Total # applications, # success')
            for col in header:
                print("|{:^20}".format(col), end='')
            print()

        # create a 2x2 table to store the data entries with col index = point val
        data_table = list()
        i = 0

        # loop through the entries in the tag list, each entry is grouped by year and has an array of docs
        # associated with it
        for year in tag_list:
            new_row = [0] * len(header)
            data_table.append(new_row)
            j = 0

            # add the year to the leftmost entry
            data_table[i][j] = year['_id']

            # add the values for each point category in the header to the data_table
            for result in year['docs']:

                # add to the total trackers
                data_table[i][len(new_row) - 1] += result['successes']
                data_table[i][len(new_row) - 2] += result['total_points']
                data_table[i][len(new_row) - 3] += result['applicants']

                # post the data entry to the table
                if data_table[i][result['point_val'] + 1] == 0:
                    data_table[i][result['point_val'] + 1] = result
                else:
                    data_table[i][result['point_val'] + 1]['applicants'] += result['applicants']
                    data_table[i][result['point_val'] + 1]['successes'] += result['successes']
                    data_table[i][result['point_val'] + 1]['total_points'] += result['total_points']

            # add 1 to i to move to next row
            i += 1

        # print the 2x2 table
        for row in data_table:
            i = 0
            for data in row:
                # printing year in first col and totals in last 2 columns
                if i == 0 or i == len(row) - 1 or i == len(row) - 2 or i == len(row) - 3:
                    print("|{:^20}".format(data), end='')
                # no data to print
                elif data == 0:
                    print("|{:^20}".format('------'), end='')
                # printing data
                else:
                    print("|{:^20}".format(str((data['applicants'], data['successes']))), end='')
                i += 1
            # printing newline
            print()

        # print the header
        if print_header:
            print('------------------- LOW LEVEL STATS PT2 FOR TAG {} ----------------------'.format(
                tag_list[0]['docs'][0]['tag_num'])
            )
            print('Values in cells are as follows: % tags awarded, % total points')
            for col in header:
                print("|{:^20}".format(col), end='')
            print()

        for row in data_table:
            i = 0
            for data in row:
                # printing year in first col and totals in last 2 columns
                if i == 0:
                    print("|{:^20}".format(data), end='')
                # no data to print
                elif data == 0 or len(row) - 3 <= i <= len(row) - 1:
                    print("|{:^20}".format('------'), end='')
                # printing data
                else:
                    perc_success = int(data['successes'] / row[len(row) - 1] * 100)
                    perc_points = int(data['total_points'] / row[len(row) - 2] * 100)
                    print("|{:^20}".format(str((perc_success, perc_points))), end='')
                i += 1
            # printing newline
            print()




dq = DrawingQueries(collection, 'moose', 'resident')
print('------------------- districts for region --------------------------')
pprint(dq.get_dist_by_reg('1'))
print('------------------- tags for district -----------------------------')
tags = dq.get_tags_by_district('111')
pprint(tags)

tag_count = 0
hl_stats = None
for tag in tags:
    if hl_stats is None:
        hl_stats = dq.get_tag_hlstats_by_year(tag['_id']['tag'])
    else:
        result = dq.get_tag_hlstats_by_year(tag['_id']['tag'])
        for _ in result:
            hl_stats.append(_)

dq.print_tag_hlstats(hl_stats, True)

print('------------------- low level stats for tag ----------------------')
llstats = dq.get_tag_llstats_by_year('111-50')
dq.print_tag_ll_stats(llstats, True)

# close the connection
connection.close()
