import pandas as pd
from resources import *

def get_correlation(writer, project_df):

    # Read config Excel file
    config_df = pd.read_excel('/mnt/c/Users/yash_kolhatkar/Desktop/Cannary/REPOSITORY/Data/source/CorrelationConfigData/metric_for_correlation_config.xlsx', sheet_name = "metric_for_correlation_config")
    conclusion_config_df = pd.read_excel('/mnt/c/Users/yash_kolhatkar/Desktop/Cannary/REPOSITORY/Data/source/CorrelationConfigData/correlation_conclusion_mapping.xlsx', sheet_name='correlation_conclusion_mapping')

    #Fill null values with NaN
    project_df["Derived Metrics Name"] = project_df["Derived Metrics Name"].fillna(project_df["Metrics Name"])
    config_df=config_df.fillna('NaN')
    conclusion_config_df = conclusion_config_df.fillna('NaN')
    
    # Create a new DataFrame to store the results
    results = []
    processed_ids = []

    #Column level Engineering Metrices tuple
    engineering_metrics = ("Unit Test Automation","CI Automation","Code Quality Automation","Regression Test Automation","Functional In Sprint Test Automtion")

    # Group project_df by 'Project ID'
    project_groups = project_df.groupby('Project ID')
    
    # Iterate over each row in the project data DataFrame
    for project_id, group in project_groups:
        if project_id in processed_ids:
            continue

        # Get unique values in the "metrics_name" column
        metric_names = group["Derived Metrics Name"].unique().tolist()


        for i in range(len(metric_names)):
            metric1 = metric_names[i]

            for j in range(i + 1, len(metric_names)):
                metric2 = metric_names[j]

                #sort Engineering and Dora metrics
                engineering_metric =  metric1 if metric1 in engineering_metrics else metric2
                dora_metric = metric2 if metric2 not in engineering_metrics else metric1

                #Get metrics data based on metrics 
                values1 = get_last_three_months(project_df, project_id, engineering_metric)
                print("Metric1 name: ",metric1)
                print("Metric1 Value:", values1)
                values2 = get_last_three_months(project_df, project_id, dora_metric)
                print("Metric2 name: ",metric1)
                print("Metric2 Value:", values1)

                # Calculate the trend using the provided function
                trend1 = calculate_diminishing_percentage(values1)
                trend2 = calculate_diminishing_percentage(values2)

                # Find the corresponding row in the config DataFrame
                config_row = config_df[((config_df['Engineering Metrics'] == engineering_metric) & (config_df['DORA Metrics'] == dora_metric))] 
                print("config_row", config_row)
                                
                if not config_row.empty:
                    proportion = config_row['Correlation'].values[0]
                    conclusion_row= assign_conclusion_from_excel(trend1, trend2, proportion, conclusion_config_df)
                    # Check if a matching row was found
                    if not conclusion_row.empty:
                        trend_outcome = conclusion_row['Trend Outcome'].values[0]
                        message = str(conclusion_row['Message to be generated'].values[0])
                        inplace_text = {
                                        "<Engineering Metrics Trend>": trend1,
                                        "<Dora Metrics Trend>": trend2,
                                        "<Engineering Metrics>" : engineering_metric,
                                        "<DORA Metrics>" : dora_metric
                                        }
                        
                        message = replace_all(message, inplace_text)
                    else:
                        trend_outcome = ""
                        message = 'Either engineering metrics trend is not increasing or values are not present'
                else:
                    continue

                result_dict = {
                    'Project ID': project_id,
                    'Engineering Metrics': engineering_metric,
                    'Dora Metrics': dora_metric,
                    'Correlation': proportion,
                    'Engineering Metrics Trend': trend1,
                    'Dora Metrics Trend': trend2,
                    'Engineering Metrics Values':', '.join(str(value) for value in values1),
                    'Dora Metrics Values':', '.join(str(value) for value in values2),
                    'Message to be generated' : message,
                    'Trend Outcome' : trend_outcome
                }
                results.append(dict(result_dict))
                processed_ids.append(project_id)
    #Create a new dataframe from the result list
    results_df = pd.DataFrame(results)

    # Write the results to a new Excel file
    results_df.to_excel(writer, sheet_name= "Engg vs DORA Metric Correlation", index=False)

    return "Document created"


def assign_conclusion_from_excel(trend1, trend2, correlation, config_df):
    # Find the matching row based on trend1, trend2, and correlation values
    matching_row = config_df[(config_df['Engineering Metrics Trend'].str.contains(trend1,case=False)) &

                             (config_df['Dora Metrics Trend'].str.contains(trend2,case=False)) &

                             (config_df['Correlation'] == correlation)]

    # Retrieve the conclusion value from the matching row
    return matching_row

