from RegObject import RegObject
from DistObject import DistObject
from TagObject import TagObject
from DrawSimul import DrawSimul
import pymongo
from pprint import pprint

# connect to the collection that we would like to query
connection = pymongo.MongoClient()
db = connection.hunting_research
collection = db.drawing_results

# region = RegObject('4', collection)
# region.set_districts()
# region.print_districts('ELK')
#
# dist_list = []
# for dist in region.districts['ELK']:
#     dist_obj = DistObject(dist, collection, 'elk', 2006, 2021)
#     dist_obj.set_tags()
#     dist_obj.print_tags_by_year()

# tag_obj = TagObject('426-20', collection, 'elk', 2015, 2021, 'resident')
# tag_obj.print_year_stats()

tag_obj = TagObject('410-20', collection, 'elk', 2015, 2021, 'resident')
# tag_obj.print_year_stats()

# for cat in tag_obj.point_stats:
#     print(cat)
#     for stats in tag_obj.point_stats[cat]:
#         print(stats)

drawing = DrawSimul(tag_obj.point_stats['num_apps'][6], tag_obj.tag, tag_obj.year_stats['num_tags'][6])
drawing.apps_ll.print_list()
print("{:20}".format("drawing simulation:"), end='')
print(drawing.run_drawing())
print("{:20}".format("actual results:"), end='')
print(tag_obj.point_stats['successes'][6])
