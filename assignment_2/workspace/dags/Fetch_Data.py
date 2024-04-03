#importing required libraries
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.request import urlopen, urlretrieve
import os
import re
import random
import shutil

#Get a Dag variable num_files which specifies how many files to download for a given year
num_files = Variable.get('num_files', default_var = 1)


def split(a, n):
    '''
    splits an array a into n mostly equal blocks
    '''
    k, m = divmod(len(a), n)
    return list(a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))

#creating a list of of years for parallel processing        
split_years_list = split(list(range(1901,2025)),6)

def download_years_list(years):
    '''
    Takes an input 'years' and and then downloads the files for the all the years. Saves each year files in their own folder.
    '''
    for l in years:
        try:
            os.makedirs(f'/opt/airflow/Files/Files/{l}')
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
        print(files)
        if num_files > 0 and num_files < len(files):
            files = random.sample(files,num_files)
        
        for i in range(len(files)):
            urlretrieve(files[i],f'/opt/airflow/Files/Files/{l}/{x[i]}')

#Zips all the files then deletes them, leaving just the zip
def zip_files():
    shutil.make_archive('/opt/airflow/Files/Files', 'zip', '/opt/airflow/Files/Files')
    shutil.rmtree('/opt/airflow/Files/Files', ignore_errors=True)

with DAG('fetch_data_files', start_date=datetime(2016, 1, 1), default_args={"retries" : 0}, catchup = False) as dag:
    
    #creates tasks based on the number of workers specified. I have specified 6.
    get_files_task = [
        PythonOperator(
            task_id = f'get_files_{i+1}',
            python_callable = download_years_list,
            op_kwargs = {'years' : split_years_list[i]}
    ) for i in range(6)]

    zip_files_task = PythonOperator(
        task_id = 'zip_files',
        python_callable = zip_files
    )

get_files_task >> zip_files_task
