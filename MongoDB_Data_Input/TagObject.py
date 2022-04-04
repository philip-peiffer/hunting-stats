# This file defines the object for the tag level queries/stats
import pymongo
from pprint import pprint

class TagObject:

    def __init__(self, tag_num: str, doc_collection: pymongo.collection.Collection, species: str, start_year: int,
                 end_year: int, residency: str):
        self.tag = tag_num
        self.doc_coll = doc_collection
        self.species = species.upper()
        self.start = start_year
        self.end = end_year
        self.residency = residency.upper()
        self.year_stats = {
            'num_apps': [0]*(self.end - self.start + 1),
            'num_tags': [0]*(self.end - self.start + 1),
            'perc_succ': [0]*(self.end - self.start + 1),
            'pts_per_app': [0]*(self.end - self.start + 1),
            'adj_apps': [0]*(self.end - self.start + 1),
        }
        self.point_stats = {
            'num_apps': [[0]*21],
            'successes': [[0]*21],
            'tags_exp': [[0]*21],
            'perc_succ': [[0]*21],
            'point_share': [[0]*21],
            'tag_share': [[0]*21],
            'dwg_chance': [[0]*21]
        }

        for _ in range(self.start, self.end):
            for cat in self.point_stats:
                self.point_stats[cat].append([0]*21)

        self.query_year_stats()
        self.query_point_stats()

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

        # now need to loop through stats and divide sum_wa_pts by sum_apps to get weighted average pts
        # also need to assign each stat to the appropriate attribute under year_stats
        for stat in stats:
            year = int(stat['_id']['year'])

            self.year_stats['num_apps'][year - self.start] = stat['sum_apps']
            self.year_stats['num_tags'][year - self.start] = stat['sum_tags']
            self.year_stats['adj_apps'][year - self.start] = stat['sum_pts']

            # calculate % success pts/app and assign it to year_stats
            perc_success = round(stat['sum_tags'] / stat['sum_apps'] * 100, 1)
            pts_per_app = round(stat['sum_wa_pts'] / stat['sum_apps'], 1)
            self.year_stats['perc_succ'][year - self.start] = perc_success
            self.year_stats['pts_per_app'][year - self.start] = pts_per_app

    def print_year_stats(self):
        """Prints all the year stats as rows"""
        # first print the header
        print('---------- Tag Stats for {} ----------'.format(self.tag))
        print('|{:^15}'.format("Query"), end='')
        for year in range(self.start, self.end+1):
            print('|{:^8}'.format(year), end='')
        print()

        # now cycle through self.year_stats and print each category
        for cat in self.year_stats:
            print('|{:^15}'.format(cat), end='')
            for num in self.year_stats[cat]:
                print('|{:^8}'.format(num), end='')
            print()
        print('\n')

    def query_point_stats(self):
        """Queries all the points stats desired. Returns a dictionary with the stats as keys. The values are lists
        where each element corresponds to a given point value."""

        pipeline = [
            {
                '$match': {'species': self.species, 'tag_num': self.tag, 'residency': self.residency,
                           'dwg_year': {'$gte': self.start, '$lte': self.end}
                           }
            },
            {
                '$group': {'_id': {'year': '$dwg_year', 'points': '$point_val'},
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

        # now loop through pt_stats and map the stats to the point_stats dictionary
        for stat in pt_stats:
            year_index = stat['_id']['year'] - self.start
            pts_index = stat['_id']['points']

            # calculate point share, tag share, % success for each point cat, and dwg chance
            pt_share = 100
            dwg_chance = 100
            if self.year_stats['adj_apps'][year_index] != 0:
                pt_share = round(stat['sum_pts'] / self.year_stats['adj_apps'][year_index] * 100, 0)

                dwg_chance = 'tbd'

            tag_share = round(stat['sum_tags'] / self.year_stats['num_tags'][year_index] * 100, 0)
            tags_expected = round(pt_share / 100 * self.year_stats['num_tags'][year_index], 0)
            perc_success = round(stat['sum_tags'] / stat['sum_apps'] * 100, 0)

            self.point_stats['num_apps'][year_index][pts_index] = stat['sum_apps']
            self.point_stats['successes'][year_index][pts_index] = stat['sum_tags']
            self.point_stats['point_share'][year_index][pts_index] = pt_share
            self.point_stats['tag_share'][year_index][pts_index] = tag_share
            self.point_stats['tags_exp'][year_index][pts_index] = tags_expected
            self.point_stats['dwg_chance'][year_index][pts_index] = dwg_chance
            self.point_stats['perc_succ'][year_index][pts_index] = perc_success
