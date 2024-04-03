import pandas as pd
import os

def process(file):
    df = pd.read_csv(file, parse_dates=['DATE'])
    output_df = df.groupby(df['DATE'].dt.month).mean()
    return output_df

def main():
    years = os.listdir('../assignment_3/Files/Files/')
    
    for year in years :
        os.makedirs(f'../assignment_3/Files/processed_dataframes/{year}', exist_ok = True)
        
        file_names = os.listdir(f'../assignment_3/Files/processing_dataframes/{year}')
        for file in file_names:
            file_path = f'../assignment_3/Files/processing_dataframes/{year}/{file}'
            processed_df = process(file_path)
            processed_df.to_csv(f'../assignment_3/Files/processed_dataframes/{year}/pro_month_avg_{file}', index=False)
        
if __name__ == "__main__":
    main()