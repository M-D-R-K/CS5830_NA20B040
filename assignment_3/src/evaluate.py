import pandas as pd
import os
from sklearn.metrics import r2_score
import json



def compare_averages(computed_file_path, extracted_file_path):
    """
    generate r2 scores of the two given dataframes
    Args:
        computed_file_path string: path of computed dataframe (monthly averages computed from dailies)
        extracted_file_path string: path of extracted dataframe

    Returns:
        dict: dictionary of r_2 scores
    """
    month_dict = {
        'MonthlyMeanTemperature' : ['DailyAverageDryBulbTemperature', 'HourlyDryBulbTemperature'],
        'MonthlySeaLevelPressure' : ['DailyAverageSeaLevelPressure', 'HourlySeaLevelPressure'],
        'MonthlyStationPressure' : ['DailyAverageStationPressure', 'HourlyStationPressure']
    }
    computed_df = pd.read_csv(computed_file_path)
    extracted_df = pd.read_csv(extracted_file_path)
    
    concat_df = pd.concat([extracted_df, computed_df], axis = 1)
    
    r2_scores = {k : {'Daily Score' : None, 'Hourly Score' : None} for k in month_dict.keys()}
    cols = concat_df.columns
    
    
    
    #compute r_2 scores of corresponding columns using the month_dict given above
    for k, v in month_dict.items():
        if k in cols:
            if v[0] in cols:
                temp_df = concat_df[[k,v[0]]]
                temp_df = temp_df.dropna()
                r2_scores[k]['Daily Score'] = r2_score(temp_df[k], temp_df[v[0]])
            if v[1] in cols:
                temp_df = concat_df[[k,v[1]]]
                temp_df = temp_df.dropna()
                r2_scores[k]['Hourly Score'] = r2_score(temp_df[k], temp_df[v[1]])
    
    return r2_scores
    
    
    
def main():
    """
    Computes and stores r2 scores  as JSON files in a directory.
    """
    years = os.listdir('../assignment_3/Files/extracted_dataframes/')
    
    for year in years:
        os.makedirs(f'../assignment_3/Files/outputs/{year}', exist_ok = True)
        
        extracted_file_names = os.listdir(f'../assignment_3/Files/extracted_dataframes/{year}')
        for file in extracted_file_names:
            extracted_path = f'../assignment_3/Files/extracted_dataframes/{year}/{file}'
            processed_path = f'../assignment_3/Files/processed_dataframes/{year}/{file}'
            scores_dict = compare_averages(processed_path,extracted_path)
            name = file.split()[0]
            with open(f'../assignment_3/Files/outputs/{year}/{name}.json', 'w') as f:
                json.dump(scores_dict, f)
               
               
                
if __name__ == "__main__":
    main()
    