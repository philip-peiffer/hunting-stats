import subprocess
import os

# this program starts all of the services that the react front end relies on to display data
curr_dir = os.getcwd()

print("Starting Queries Server...")
queries_command = curr_dir + '\Queries\server.py'
subprocess.Popen(['py', queries_command], creationflags=subprocess.CREATE_NEW_CONSOLE)

print("Starting Forecast Server...")
forecast_path = curr_dir + '\Forecast_Applicants\CS361_forecast_applicants\calc_odds.py'
forecast_command = 'py ' + forecast_path
subprocess.Popen(['py', forecast_path], creationflags=subprocess.CREATE_NEW_CONSOLE)

print("Starting Drawing Simulation Server...")
ds_path = curr_dir + '\Drawing_Simulation\server.py'
subprocess.Popent(['py', ds_path], creationflags=subprocess.CREATE_NEW_CONSOLE)