# hunting-stats
This repository contains code used to create a Mongo database to store hunting stats for the state of MT. 
It also contains code to query those stats and return them via an HTTP based API.

# Directory Structure
## MongoDB_Data_Input
The Data_Input directory contains a script used to strip the FWP data for drawing results from an excel format and upload the 
data to a local MongoDB collection. 

## Queries
This directory contains a server script that can be used to query the drawing results from mongo via an HTTP API format. It also contains
the various object definitions that are needed in order for the API to work.
