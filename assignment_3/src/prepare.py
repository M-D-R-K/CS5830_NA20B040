import pandas as pd
import os

def extract_monthly_averages(file):
    month_averages = ['MonthlyMeanTemperature', 'MonthlySeaLevelPressure', 'MonthlyStationPressure']
    hourly_averages = ['HourlyDryBulbTemperature', 'HourlySeaLevelPressure', 'HourlyStationPressure']
    daily_averages = ['DailyAverageDryBulbTemperature', 'DailyAverageSeaLevelPressure', 'DailyAverageStationPressure']
    
    df = pd.read_csv(file, parse_dates=['DATE'])
    df = df.select_dtypes(include=['datetime64[ns]', 'float64'])
    df.dropna(how='all', axis=1, inplace=True)
    
    extract_cols = ['DATE'] 
    compute_cols = ['DATE']
    for i in range(len(month_averages)):
        if month_averages[i] in df.columns:
            
            if daily_averages[i] in df.columns:
                compute_cols.append(daily_averages[i])
                extract_cols.append(month_averages[i])
            
            elif hourly_averages[i] in df.columns:
                compute_cols.append(hourly_averages[i])
                extract_cols.append(month_averages[i])
    
    extracted_df = df[extract_cols].groupby(df['DATE'].dt.month).max()
    
    compute_df = df[compute_cols]
    return extracted_df, compute_df

def main():
    years = os.listdir('../assignment_3/Files/Files/')
    
    for year in years :
        os.makedirs(f'../assignment_3/Files/extracted_dataframes/{year}', exist_ok = True)
        os.makedirs(f'../assignment_3/Files/processing_dataframes/{year}', exist_ok = True)
        
        file_names = os.listdir(f'../assignment_3/Files/Files/{year}')
        
        for file in file_names:
            file_path = f'../assignment_3/Files/Files/{year}/{file}'
            extracted_df, processing_df = extract_monthly_averages(file_path)
            extracted_df.to_csv(f'../assignment_3/Files/extracted_dataframes/{year}/ex_month_avg_{file}', index=False)
            processing_df.to_csv(f'../assignment_3/Files/processing_dataframes/{year}/processing_{file}', index=False)
        
    
    
if __name__ == "__main__":
    main()

    
    
        