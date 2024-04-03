import os
import multiprocessing 
import random
import re
import yaml
import pandas as pd
import shutil

from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup


def split(a, n):
    """
    splits an array a into n mostly equal blocks
    Args:
        a list: an array of integers
        n int: number of blocks to divide a into

    Returns:
        list: list of lists of a divided into n parts
    """
    k, m = divmod(len(a), n)
    return list(a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))



def download_years_list(years, num_files):
    """
    Takes an input 'years' and and then downloads the files for the all the years. Saves each year files in their own folder.
    Args:
        years List: List of years
        num_files int: number of files to download from each year
    """
    for l in years:
        try:
            os.makedirs(f'../assignment_3/Files/Files/{l}')
        except FileExistsError:
            pass
        
        parenturl = f"https://www.ncei.noaa.gov/data/local-climatological-data/access/"
        url = parenturl + f"{l}/"
        page = urlopen(url)
        html = page.read().decode("utf-8")
        
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text()
        
        x = re.findall("\d{11}.csv", text)
        files = [url + f"{i}" for i in x]
        if num_files > 0 and num_files < len(files):
            files = random.sample(files,num_files)
        print(f"The following files will be downloaded for the year {l}")
        for file in files:
            print(file)
        for i in range(len(files)):
            urlretrieve(files[i],f'../assignment_3/Files/Files/{l}/{x[i]}')
        
        downloaded_files = os.listdir(f'../assignment_3/Files/Files/{l}')
        downloaded_files = [f'../assignment_3/Files/Files/{l}/' + x for x in downloaded_files]
        for i in range(len(downloaded_files)):
            if check_monthly_aggregates(downloaded_files[i]):
                os.remove(downloaded_files[i])



def check_monthly_aggregates(data_path):
    """
    Function to check whether the monthly columns are completely empty or not. Returns True if empty
    Args:
        data string: path to the data file

    Returns:
        bool: Are all the monthly fields completely empty? If yes, return True
    """
    df = pd.read_csv(data_path)
    df.dropna(how='all', axis=1, inplace=True)  #drop all empty columns
    cols= df.columns
    monthly_cols = [col[7:] for col in cols if re.search('Monthly', col)] #use re to search for fields containing the word Monthly which is 7 letters long.
    
    if monthly_cols == []:
        return True
    else:
        return False



def main():
    """
    Creates multiple Processes(upto 4): To download the list of years given in the params.yaml folder
    """
    params = yaml.safe_load(open("params.yaml"))['download']
    year = params['year']
    n_locs = params['n_locs']
    
    years = split(year,4) #Divide the years into 4 parts for parallel downloading
    years = [x for x in years if x !=[]]
    processes = []
    
    if len(years) < 4:
        for i in range(len(years)):
            processes.append(multiprocessing.Process(target=download_years_list, args=(years[i], n_locs)))
    else:
        for i in range(4):
            processes.append(multiprocessing.Process(target=download_years_list, args=(years[i], n_locs)))
    
    for p in processes:
        p.start()
        
    for p in processes:
        p.join()
    
    folders = os.listdir('../assignment_3/Files/Files/')
    folders = ['../assignment_3/Files/Files/' + year for year in folders]
    
    for i in range(len(folders)):
        if len(os.listdir(folders[i])) == 0: # Check if the folder is empty
            shutil.rmtree(folders[i]) # If so, delete it
    


if __name__ == "__main__":
    main()