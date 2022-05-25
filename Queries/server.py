
from RegObject import RegionsObject
from DistObject import DistObject
from TagObject import TagObject
from DrawSimul import DrawSimul
from flask import Flask, jsonify
import pymongo

# initialize the app
app = Flask(__name__)

# connect to the collection that we would like to query
connection = pymongo.MongoClient()
db = connection.hunting_research
collection = db.drawing_results

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
    region = RegionsObject(res_choice, spec_choice, collection, 2021)
    region.set_data()

    # send the appropriate data back
    return_object = {'data': region.data}
    return return_object


@app.route('/residency/<res_choice>/species/<spec_choice>/region/<reg_choice>/districts')
def get_district_stats(res_choice, spec_choice, reg_choice):

    res_choice = reformat_residency(res_choice)

    # create a districts object that has all the districts for the given species
    districts = DistObject(collection, spec_choice, res_choice, reg_choice, 2021)
    districts.query_districts_data()
    return {'data': districts.year_stats}


@app.route('/residency/<res_choice>/species/<spec_choice>/region/<reg_choice>/district/<dist_choice>/tags')
def get_tag_stats(res_choice, spec_choice, reg_choice, dist_choice):

    res_choice = reformat_residency(res_choice)

    # get a list of tags within the district
    districts = DistObject(collection, spec_choice, res_choice, reg_choice, 2021)
    tag_nums = districts.get_tags(dist_choice)

    # loop through the tags and run queries on each tag, adding the results to the tags list
    tags = []
    for result in tag_nums:
        tag_num = result['_id']['tag num']
        tag_obj = TagObject(tag_num, collection, spec_choice, 2017, 2021, res_choice)
        tags.append(tag_obj.convert_to_dict())

    return {'data': tags}


@app.route('/residency/<res_choice>/species/<spec_choice>/tags/<tag_id>')
def get_tag(res_choice, spec_choice, tag_id):
    
    res_choice = reformat_residency(res_choice)

    tag_obj = TagObject(tag_id, collection, spec_choice, 2017, 2021, res_choice, True)
    return {'tag exists': tag_obj.exists}


app.run()
