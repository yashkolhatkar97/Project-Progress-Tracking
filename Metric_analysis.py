#!/usr/bin/env python
# coding: utf-8

#---------------------------------------------------------------------------------------------------
# Script: Metrics alert and report generation
# Description: Script to get the project ID's from project database and for each project ID 
#              get the configurations provided in configuration excel and based on the methods   
#              provided in configurations calculations has been done and output has been generated
#                          
# Date: 12th May, 2023
# Initial Author: Yash Kolhatkar
# Domain Expert : 
# 
#
# Revisions:
# Date         |   Author              | Comments
#-----------------------------------------------------------------------------------------------


from flask import Flask, request
import pandas as pd
from resources import *
from datetime import datetime, timedelta
import json
import logging
from correlation import *

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='/mnt/c/Users/yash_kolhatkar/Desktop/Cannary/REPOSITORY/Logs/app.log')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s',
                    filename='/mnt/c/Users/yash_kolhatkar/Desktop/Cannary/REPOSITORY/Logs/app.log')

# Creating an object 
logger = logging.getLogger()

# Set the log level to DEBUG
app.logger.setLevel(logging.DEBUG)

# Setting the threshold of logger to DEBUG 
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
app.logger.addHandler(handler)


def get_projects_configurations():
    dt = datetime.now()
    logging.info("Inside get projects function")
    logging.info("timestamp at the start: {}".format(dt))
    
    # Get the arguments from the query string
    project_file_path = request.args.get('project_file_path')
    config_file_path = request.args.get('config_file_path')

    # Load the project data into a pandas DataFrame
    project_df = pd.read_excel(project_file_path, sheet_name='Master Metrics Sheet')
    project_df = project_df.sort_values(by=['Project ID'])
    project_df["Derived Metrics Name"] = project_df["Derived Metrics Name"].fillna(project_df["Metrics Name"])

    # Load the config data into a pandas DataFrame
    config_df = pd.read_excel(config_file_path, sheet_name='Metric_Configuration')
    logging.info("config df:{config_df}")

    # Get the unique metrics names from the config data
    unique_derived_metrics_names = config_df['Derived Metrics Name'].unique()

    # Initialize an empty list to store the result
    config_dict = []

    # Loop through the project data for the metrics names present in the config data
    for derived_metrics_name in unique_derived_metrics_names:
        logging.debug(f"Processing metrics name: {derived_metrics_name}")
        
        # Filter the project data for the current metrics name
        filtered_project_df = project_df[project_df['Derived Metrics Name'] == derived_metrics_name]
        if len(filtered_project_df) == 0:
            continue

        # Merge the filtered project data and config data on the 'Metrics Name' column
        merged_df = pd.merge(filtered_project_df, config_df, on='Derived Metrics Name', how = "inner")
        merged_df.drop_duplicates()
        logging.info(f"merged df : {merged_df}")

        # Loop through the merged data one by one
        for index, row in merged_df.iterrows():
            logging.debug("Inside for loop")

            # Get the project ID and current metric value
            project_id = row['Project ID']
            current_metric_value = row['Metrics Data']

            # Convert the row data to a dictionary
            row_data = row.to_dict()

            # Get the last three months metrics values for the current project and metrics
            three_months_metrics_values = get_last_three_months(project_df, project_id, derived_metrics_name)
            if not three_months_metrics_values:
                continue

            if row['Calculation Method'] == 'last 3 months':
                alert = calculate_diminishing_percentage(three_months_metrics_values)
                # merged_df = merged_df[(merged_df['Project ID'] == project_id)]
                filtered_merged_df = merged_df[(merged_df['Derived Metrics Name'] == derived_metrics_name) & (merged_df['Trend Engg Metrics'] == alert)]
                filtered_merged_df = filtered_merged_df[(filtered_merged_df['Project ID'] == project_id)]
                # Convert the row data to a dictionary
                for index, row in filtered_merged_df.iterrows():
                    row_data = row.to_dict()

            # Add the project information and config data to the result list
            config_dict.append({'Project ID': project_id, 'Project Name': row['Project Name'], 'Metrics Name':row['Metrics Name'], 'Derived Metrics Name': derived_metrics_name, 
                                'Metric_values': {'current month metric': [current_metric_value],
                                                  'last three months metrics': three_months_metrics_values},
                                'config': row_data})
            logging.debug("config dict created")

        
    # Return the result list as a JSON response and project dataframe
    project_config = config_dict
    logging.info("returning project config and project df")
    logging.debug("project config type: %s", type(project_config))
    dt = datetime.now()
    logging.info("timestamp at the end: {}".format(dt))
    return project_config, project_df


@app.route('/projects', methods=['GET'])
def alert_message_generation():
    dt = datetime.now()
    logging.info("timestamp at the start of alert message function: {}".format(dt))
    try:
        project_config, project_df = get_projects_configurations()
        alert_msg = []
        project_config_updated = []
        for data in project_config:
            logging.info("inside 1st for loop")
            method = data['config']['Calculation Method']
            direction = data['config']['Direction']
            lsl =data['config']['LSL']
            usl =data['config']['USL']
            last_three_months_metrics = data['Metric_values']['last three months metrics']
            current_month_metrics = data['Metric_values']['current month metric']
            print("method type:",type(method))
            print("Method:",method)
            if method == "last 3 months":
                alert = calculate_diminishing_percentage(last_three_months_metrics)
                threshold_output = analyze_metrics_threshold(direction, lsl, usl, last_three_months_metrics)
                logging.debug("alert message: %s", alert)
                logging.debug("alert message type: %s", type(alert))
                data['config'].update({'Engineering metrics trend': alert, 'Metrics Values': last_three_months_metrics, "Threshold Output" : threshold_output})
                print("data dict", data)
                alert_msg.append(alert)
            elif method == "High %":
                min_thresh = data['config']["LSL"]
                max_thresh = data['config']["USL"]
                alert = calculate_high_percentage(current_month_metrics, min_thresh, max_thresh)
                data['config'].update({'Engineering metrics trend': alert, 'Metrics Values': current_month_metrics})
                alert_msg.append(alert)
            else:
                alert = dummy_method()
                logging.debug("alert message: %s", alert)
                continue                            #ignore the dummy method 
            project_config_updated.append(data)

        writer = write_json_to_excel(project_config_updated)
        get_correlation(writer, project_df)
        writer.close()
        dt = datetime.now()
       
        logging.info("timestamp at the end of alert message function: {}".format(dt))
        return {"alert_messages": alert_msg}  # returning a dictionary with alert messages
    except ImportError as e:
        logging.error("Could not import the necessary module: %s", e)
    except Exception as e:
        logging.error("An error occurred: %s", e)
        return {"error": str(e)}  # returning a dictionary with error message


if __name__ == '__main__':
    app.run(debug=True)
