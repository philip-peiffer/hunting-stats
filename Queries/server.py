
from RegObject import RegionsObject
from DistObject import DistObject
from TagObject import TagObject
from flask import Flask, jsonify
import pymongo

# initialize the app
app = Flask(__name__)

# connect to the collection that we would like to query
connection = pymongo.MongoClient()
db = connection.hunting_research
collection = db.drawing_results

# define constants used throughout 
END_YEAR = 2021

def reformat_residency(res_choice: str):
    """Reformats the residency choice to match what is required for the queries"""
    if res_choice[:2].upper() == "NON":
        return "nonresident"
    else:
        return "resident"


# define CORS policy
@app.after_request
def after_request(response):
    header = response.headers
    header['Access-Control-Allow-Origin'] = '*'
    return response


# define routes
@app.route('/residency/<res_choice>/species/<spec_choice>/regions_stats')
def get_region_stats(res_choice, spec_choice):

    res_choice = reformat_residency(res_choice)

    # create a region object that has all the regions for the given species
    return_object = {'data': []}
    for region in range(1, 8):
        new_region = RegionsObject(res_choice, spec_choice, collection, END_YEAR, str(region))
        return_object['data'].append(new_region.convert_to_dict())
    
    # send the appropriate data back
    return return_object


@app.route('/residency/<res_choice>/species/<spec_choice>/region/<reg_choice>/districts')
def get_district_stats(res_choice, spec_choice, reg_choice):

    res_choice = reformat_residency(res_choice)

    # create a region object and query for the districts within that region
    results = RegionsObject(res_choice, spec_choice, collection, END_YEAR, reg_choice, False).get_districts()

    # loop through districts and create a district object for each entry, querying stats
    districts = []
    for result in results:
        district = result['_id']['district']
        dist_obj = DistObject(collection, spec_choice, res_choice, district, END_YEAR)
        districts.append(dist_obj.convert_to_dict())
    
    return {'data': districts}


@app.route('/residency/<res_choice>/species/<spec_choice>/region/<reg_choice>/district/<dist_choice>/tags')
def get_tag_stats(res_choice, spec_choice, reg_choice, dist_choice):

    res_choice = reformat_residency(res_choice)

    # get a list of tags within the district
    district = DistObject(collection, spec_choice, res_choice, dist_choice, END_YEAR, False)
    tag_nums = district.get_tags()

    # loop through the tags and run queries on each tag, adding the results to the tags list
    tags = []
    for result in tag_nums:
        tag_num = result['_id']['tag num']
        tag_obj = TagObject(tag_num, collection, spec_choice, 2017, END_YEAR, res_choice)
        tags.append(tag_obj.convert_to_dict())

    return {'data': tags}


@app.route('/residency/<res_choice>/species/<spec_choice>/tags/<tag_id>')
def get_tag(res_choice, spec_choice, tag_id):
    
    res_choice = reformat_residency(res_choice)

    tag_obj = TagObject(tag_id, collection, spec_choice, 2017, END_YEAR, res_choice, True)
    return {'tag exists': tag_obj.exists}


@app.route('/residency/<res_choice>/species/<spec_choice>/tags/<tag_num>/stats')
def get_ind_tag_stats(res_choice, spec_choice, tag_num):
    # create a tag object for the queried tag
    tag_obj = TagObject(tag_num, collection, spec_choice, 2017, END_YEAR, res_choice)
    data = tag_obj.convert_to_dict()

    return ({'data': [data]})

app.run()
