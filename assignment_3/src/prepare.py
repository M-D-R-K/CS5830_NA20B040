import pandas as pd
import os



def extract_monthly_averages(file_path):
    """
    Extracts the monthly averages and the hourly and daily fields corresponding to it and stores them in separate dataframes in separate locations
    Args:
        file_path string: path of csv file

    Returns:
        dataframe: two dataframes, one contains the extracted monthly columns and the other contains the daily and hourly columns.
    """
    month_averages = ['MonthlyMeanTemperature', 'MonthlySeaLevelPressure', 'MonthlyStationPressure']
    hourly_averages = ['HourlyDryBulbTemperature', 'HourlySeaLevelPressure', 'HourlyStationPressure']
    daily_averages = ['DailyAverageDryBulbTemperature', 'DailyAverageSeaLevelPressure', 'DailyAverageStationPressure']
    
    df = pd.read_csv(file_path, parse_dates=['DATE'])
    df = df.select_dtypes(include=['datetime64[ns]', 'float64'])
    df.dropna(how='all', axis=1, inplace=True)
    
    extract_cols = ['DATE'] 
    compute_cols = ['DATE']
    for i in range(len(month_averages)):
        if month_averages[i] in df.columns:
            
            if daily_averages[i] in df.columns:
                compute_cols.append(daily_averages[i])
                extract_cols.append(month_averages[i])
            
            if hourly_averages[i] in df.columns:
                compute_cols.append(hourly_averages[i])
                extract_cols.append(month_averages[i])
    
    extracted_df = df[extract_cols].groupby(df['DATE'].dt.month).max()
    
    compute_df = df[compute_cols]
    return extracted_df, compute_df



def main():
    """
    Extracts required Fields in two dataframes and stores them as csvs in two different locations
    """
    years = os.listdir('../assignment_3/Files/Files/')
    
    for year in years :
        os.makedirs(f'../assignment_3/Files/extracted_dataframes/{year}', exist_ok = True)
        os.makedirs(f'../assignment_3/Files/processing_dataframes/{year}', exist_ok = True)
        
        file_names = os.listdir(f'../assignment_3/Files/Files/{year}')
        
        for file in file_names:
            file_path = f'../assignment_3/Files/Files/{year}/{file}'
            extracted_df, processing_df = extract_monthly_averages(file_path)
            extracted_df.to_csv(f'../assignment_3/Files/extracted_dataframes/{year}/{file}', index=False)
            processing_df.to_csv(f'../assignment_3/Files/processing_dataframes/{year}/{file}', index=False)
        
    
    
if __name__ == "__main__":
    main()

    
    
        