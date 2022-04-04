# This file defines the object for the district level queries/stats
import pymongo


class DistObject:

    def __init__(self, district: str, doc_collection: pymongo.collection.Collection, species: str, start_year: int,
                 end_year: int):
        self.start = start_year
        self.end = end_year
        self.dist = district
        self.doc_coll = doc_collection
        self.species = species.upper()
        self.tags = dict()

    def set_tags(self):
        """Sets the self.tags attribute to a list of tags available in the district for the given species."""
        # run a query to match the district, group it by tag number
        pipeline = [
            {
                '$match': {'species': self.species, 'district': self.dist}
            },
            {
                '$group': {'_id': {'dwg_year': '$dwg_year', 'tag': '$tag_num'}}
            },
            {
                '$sort': {'_id': pymongo.ASCENDING}
            }
        ]

        tags = self.doc_coll.aggregate(pipeline)

        for result in tags:
            dwg_year = int(result['_id']['dwg_year'])
            tag_num = result['_id']['tag']

            try:
                self.tags[tag_num].append(dwg_year)
            except KeyError:
                self.tags[tag_num] = []
                self.tags[tag_num].append(dwg_year)

    def print_tags_by_year(self):
        """Prints the tags that were availabe for a district for each year"""

        # print the years as a header first
        print('-------- Tags by Year for District {} --------'.format(self.dist))
        print("|{:^8}".format("tag"), end='')
        for num in range(self.start, self.end+1):
            print("|{:^8}".format(num), end='')
        print()

        # now loop through the tags dictionary and print the array associated with each tag num
        for tag in self.tags:
            # first we need to convert the tag info to something we can print
            row = [''] * (self.end - self.start+1)
            for year in self.tags[tag]:
                if self.start <= year <= self.end:
                    row[year - self.start] = "X"

            # print the tag number first
            print("|{:^8}".format(tag), end='')

            # loop through the row and print
            for entry in row:
                print("|{:^8}".format(entry), end='')

            print()
        print()
