import pandas as pd
import os



def process(file_path):
    """
    Computes monthly averages using hourly/daily data from the given csv file
    Args:
        file_path string: path to the csv file

    Returns:
        pandas dataframe: dataframe of computed monthly averages
    """
    df = pd.read_csv(file_path, parse_dates=['DATE'])
    output_df = df.groupby(df['DATE'].dt.month).mean()
    return output_df



def main():
    """
    Creates and stores dataframes of each of the files Files/processing_dataframes in another subdirectory
    """
    years = os.listdir('../assignment_3/Files/Files/')
    
    for year in years :
        os.makedirs(f'../assignment_3/Files/processed_dataframes/{year}', exist_ok = True)
        
        file_names = os.listdir(f'../assignment_3/Files/processing_dataframes/{year}')
        for file in file_names:
            file_path = f'../assignment_3/Files/processing_dataframes/{year}/{file}'
            processed_df = process(file_path)
            processed_df.to_csv(f'../assignment_3/Files/processed_dataframes/{year}/{file}', index=False)
        
        
        
if __name__ == "__main__":
    main()