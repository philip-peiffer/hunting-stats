import DrawSimul as ds 
from flask import Flask, request, json

# initialize the app
app = Flask(__name__)

# define constants 
NUM_DWGS = 10

# define CORS policy
@app.after_request
def after_request(response):
    header = response.headers
    header['Access-Control-Allow-Origin'] = '*'
    return response


# define routes
@app.route('/predictions', methods=["POST"])
def get_point_predictions():

    # get the predictions array that microservice calculated and number of tags from body of request
    request_data = request.get_json()
    next_year_apps = request_data["calculated"]
    num_tags = request_data["num tags"]
    tag_id = request_data["tag"]

    # generate 10 drawing simulation objects to run the drawing 10 times
    all_simulations = []
    for _ in range(NUM_DWGS):
        simulation = ds.DrawSimul(next_year_apps, tag_id, num_tags)
        all_simulations.append(simulation)

    # loop through and run drawing simulation for each simulation object, replacing the simulation object with the result
    for i, sim in enumerate(all_simulations):
        all_simulations[i] = sim.run_drawing()
    
    # loop through results and add them all together
    total_results = [0 for _ in all_simulations[0]]
    for index in range(len(total_results)):
        for iteration in range(len(all_simulations)):
            total_results[index] += all_simulations[iteration][index]
    
    # calculate % chance of drawing
    perc_chance_of_success = []
    for i, result in enumerate(total_results):
        if next_year_apps[i] == 0:
            perc_chance = 0
        else:
            perc_chance = round((result / (next_year_apps[i] * 10)) * 100, 1)

        perc_chance_of_success.append(perc_chance)
        request_data["total tags obtained"] = total_results
        request_data["calculated success perc"] = perc_chance_of_success

    return json.jsonify(request_data)


app.run(debug=True, host='localhost', port=58555)
