# Description: This program adds documents to a mongoDB database for moose, sheep, and goat drawing results
from pymongo import MongoClient     # for connection to mongo
import os                           # for opening/writing files
import pandas as pd                 # for reading/writing excel files
import dns                          # for connection to mongo


# define function to process the data in each file
def process_line(year: int, line: list, data_indices: dict):
    """
    This function takes 2 parameters, the year of the current file, and an array that contains the contents of a
    line from a file. The function processes the data in the line depending on the year. FWP changes formats of
    data from the drawings every few years, so data processing requires checking the year to determine where the
    data lies. The function returns a list of normalized data (only the data that we care about) so that the data
    can be handled the same after the function call.
    The returned data list is as follows: [year, license number, license type, tag number, residency, point value, number of
    applicants, number of successes]
    :param data_indices:
    :param year: integer
    :param line: list
    :return: list of "normalized data"
    """
    imp_data = []
    # skip any entries that don't have residency assigned to them - these are totals, we'll use mongo to total
    if line[data_indices['residency']] == '' or line[data_indices['residency']] == ' ':
        return imp_data

    # grab the data indexes from the line that we care about
    else:
        # expand the item type - description columns if necessary (these columns contain both the license number
        # and license type with a " - " between them
        license_num = line[data_indices['license_num']]
        license_type = line[data_indices['license_type']]
        if data_indices['license_num'] == data_indices['license_type']:
            license_num, license_type = license_num.split(' - ')

        # check the tag_num field to weed out extraneous info
        tag_num = line[data_indices['tag_num']]
        if len(line[data_indices['tag_num']]) > 6:
            tag_num = line[data_indices['tag_num']].split(' ')[0]

        # check the point_val field, if it's blank then assign it to 0 points
        point_val = line[data_indices['point_val']]
        if point_val == '' or point_val == ' ':
            point_val = 0
        else:
            point_val = int(float(point_val))

        # pull out species from license_type
        species = license_type.split(' ')[0]

        # pull out district from tag_num
        district = tag_num.split('-')[0]

        imp_data = [
            year,
            species,
            int(license_num),
            license_type,
            district,
            tag_num,
            line[data_indices['residency']],
            point_val,
            int(line[data_indices['applicants']]),
            int(line[data_indices['successes']])
        ]

    return imp_data


def parse_header(header_line: str):
    """
    This function requires 1 input - the header line as a string. The function parses the header line for keywords
    to determine where the important data lies in a file. It then returns a dictionary with the important data
    locations as values (the important data names are the keys).
    :param header_line:
    :return:
    """
    header_array = header_line.split(',')
    data_locations = dict()

    # loop through the items in the header_array, searching for keywords
    i = 0
    for header in header_array:
        if header.find("Item Type") != -1:
            data_locations["license_num"] = i

        if header.find("Description") != -1:
            data_locations["license_type"] = i

        elif header.find("District") != -1:
            data_locations["tag_num"] = i

        elif header.find("Residency") != -1:
            data_locations["residency"] = i

        elif header.find("Points") != -1:
            data_locations["point_val"] = i

        elif header.find("Appl") != -1:
            data_locations["applicants"] = i

        elif header.find("# Success") != -1 or header.find("Number of Success") != -1:
            data_locations["successes"] = i

        i += 1

    return data_locations


def main():
    # connect to the mongoDB server -- uses Localhost port 27017 by default
    connection = MongoClient()

    print(connection.list_database_names())

    # access the database
    db = connection.hunting_research

    # access the collection for drawing results
    collection = db.drawing_results

    clear_collection = False
    if clear_collection:
        collection.drop()

    # get the directory path from the user
    root_path = "C:\\Users\\phili\\OneDrive\\Documents\\Hunting Research\\"
    dir_name = input("Please enter directory name: ")
    dir_path = root_path + dir_name

    # open the directory
    with os.scandir(dir_path) as entries:
        for entry in entries:
            if entry.is_file():
                entry_name_array = entry.name.split(' ')
                entry_path = dir_path + '\\' + entry.name

                # skip all non excel files
                if entry.name.find(".xls") == -1:
                    continue

                # open the file and use pandas to read it / convert it to csv
                with open(entry_path, 'rb') as curr_file:
                    df = pd.read_excel(curr_file)
                    new_file_path = dir_path + '\\' + entry_name_array[0] + ".csv"
                    df.to_csv(new_file_path)


                # for each csv file, loop through file lines, creating a new document for each line
                with open(new_file_path, 'r') as csv_file:
                    print("processing year: ", int(entry_name_array[0]))
                    data_chunk = []
                    data_locations = dict()
                    past_header = False
                    data_for_mongo = False

                    for line in csv_file:
                        # headers all have "Item Type" in one of the columns, so skip until you hit that header
                        # once you hit the header, parse it to find out what indexes you should be using in order
                        # to find the correct data since the format changes from year to year
                        if line.find("Item") != -1:
                            past_header = True
                            data_locations = parse_header(line)
                            continue

                        # split the line into the different columns
                        line_array = line.split(',')

                        # If we're past the header, we can finally process the data.
                        # File formats changed in 2017, so need to see what year we're in
                        if past_header:
                            imp_data = process_line(int(entry_name_array[0]), line_array, data_locations)

                            # if imp_data contains data, add this data to chunk that we're going to upload to mongo
                            if imp_data:
                                new_dict = {
                                    'dwg_year': imp_data[0],
                                    'species': imp_data[1],
                                    'license_num': imp_data[2],
                                    'license_type': imp_data[3],
                                    'district': imp_data[4],
                                    'tag_num': imp_data[5],
                                    'region': imp_data[5][0],
                                    'residency': imp_data[6],
                                    'point_val': imp_data[7],
                                    'applicants': imp_data[8],
                                    'successes': imp_data[9]
                                }
                                # adding in total_points key-value, which is the adjusted point basis
                                adjusted_basis = new_dict['point_val'] ** 2
                                if new_dict['point_val'] == 0:
                                    adjusted_basis = 1
                                new_dict['total_points'] = new_dict['applicants'] * adjusted_basis
                                data_chunk.append(new_dict)
                                data_for_mongo = True

                            # if no data in imp_data then we've hit a last line so upload what you've got to mongo and
                            # reset everything
                            else:
                                if data_for_mongo:
                                    collection.insert_many(data_chunk)
                                data_chunk = []
                                data_for_mongo = False

    # close the connection
    connection.close()


if __name__ == "__main__":
    main()
