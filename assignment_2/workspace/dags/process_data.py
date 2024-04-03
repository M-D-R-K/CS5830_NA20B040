#importing Required libraries
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable


import airflow
import pandas as pd
import datetime
import shutil
import os
import zipfile
import matplotlib.pyplot as plt
import matplotlib as mpl
import geopandas
from geodatasets import get_path

#Dag variable columns. Specifies which columns to use to create the maps. default is all the numerical columns.
cols = Variable.get('cols_to_use', default_var = ['DATE', 'LATITUDE', 'LONGITUDE', 'HourlyDryBulbTemperature', 
                                                  'HourlySeaLevelPressure','HourlyVisibility', 
                                                  'HourlyWindSpeed'])

#getting a world map using geodatasets
world = geopandas.read_file(get_path("naturalearth.land"))

def split(a, n):
    k, m = divmod(len(a), n)
    return list(a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))
        
split_years_list = split(list(range(1901,2025)),6)

default_args = {
    "depends_on_past" : False,
    "start_date"      : airflow.utils.dates.days_ago( 1 ),
    "retries"         : 0,
    "retry_delay"     : datetime.timedelta( hours= 5 ),
}

#Does the opposite of the zip_files function
def unarchive():
    try:
        os.makedirs('/opt/airflow/Files/archive')
    except FileExistsError:
        pass
    if zipfile.is_zipfile('/opt/airflow/Files/Files.zip'):
        shutil.unpack_archive('/opt/airflow/Files/Files.zip','/opt/airflow/Files/archive', "zip")
    else:
        raise Exception("Not a Valid Zip file")


def get_avg_csv(years):
    '''
    takes input 'years'. For each year, takes all the csv, computes the monthly averages of all columns of each csv and then merges them and saves them
    in the same folder as 'YYYY_merged.csv'. The merged dates, now go from 1 to 12, corresponding to each day of the month
    '''
    for year in years:
        year = str(year)
        path = "/opt/airflow/Files/archive/" + year 
        dfs = []
        files = os.listdir(path)
        for file in files:
            df = pd.read_csv(path+'/'+file, usecols = cols, parse_dates=['DATE'])
            for col in cols[1:]:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df.groupby(df['DATE'].dt.month).mean()
            dfs.append(df)
        if len(dfs) == 1:
            df = dfs[0]
        else:
            df = pd.concat(dfs)
        df.to_csv(path + f'/{year}_merged.csv')

def map_maker(years, cols = ['HourlyDryBulbTemperature', 'HourlySeaLevelPressure','HourlyVisibility', 'HourlyWindSpeed']):
    '''
    takes 2 inputs:
    years -> list of years to manipulate (used for parallelization)
    cols -> which cols to plot images of.
    Function takes the merged dataset for each year, then plots it based on date.
    '''
    month_dict = {
    1 : "Jan",
    2 : "Feb",
    3 : "Mar",
    4 : "Apr",
    5 : "May",
    6 : "Jun",
    7 : "Jul",
    8 : "Aug",
    9 : "Sep",
    10 : "Oct",
    11 : "Nov",
    12 : "Dec"
    }
    cols_titles_dict = {
        'HourlyDryBulbTemperature' : ['Average Dry Bulb Temperature', '°C'],
        'HourlySeaLevelPressure' : ['Average Sea Level Pressure', 'Pa'],
        'HourlyVisibility' : ['Average visibility', "m"],
        'HourlyWindSpeed' : ['Average Windspeed', 'm/s']
    }
    for year in years:
        try:
            os.makedirs(f'/opt/airflow/Files/images/{year}')
        except:
            pass
        df1 = pd.read_csv(f'/opt/airflow/Files/archive/{year}/{year}_merged.csv')
        for num in range(1,13):
            df = df1.loc[df1['DATE'] == num]
            gdf = geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(df.LONGITUDE, df.LATITUDE), crs="EPSG:4326")
            for col in cols:
                try:
                    max_val = gdf[col].max()
                    min_val = gdf[col].min()
                    val_range = max_val - min_val
                    min_x = round(gdf['LONGITUDE'].min()-5)
                    min_y = round(gdf['LATITUDE'].min()-5)
                    max_x = round(gdf['LONGITUDE'].max()+5)
                    max_y = round(gdf['LATITUDE'].max()+5)
                    fig, ax = plt.subplots(figsize=(6,6))
                    ax.set_title(f'Plot of {cols_titles_dict[col][0]} For {month_dict[num]}, {year}')
                    world.clip([min_x, min_y, max_x, max_y]).plot(ax = ax, color="white", edgecolor="black")
                    gdf.plot(ax=ax, c = gdf[col], cmap = 'magma', vmin = round(min_val-0.1*val_range), vmax = round(max_val + 0.1*val_range))
                    fig.colorbar(mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(round(min_val-0.1*val_range),round(max_val + 0.1*val_range)), cmap='magma'),
                                ax=ax, orientation='vertical', label=f'{cols_titles_dict[col][0]} ({cols_titles_dict[col][1]})')
                    plt.savefig(f'/opt/airflow/Files/images/{year}/{month_dict[num]}_{year}_{col}.png')
                except:
                    continue

#simply deletes the archive
def del_archive():
    shutil.rmtree('/opt/airflow/Files/archive', ignore_errors=True)

with airflow.DAG( "processing", default_args= default_args, schedule_interval= None, catchup = False) as dag:
    
    #Sensor that probes for the given 'Files.zip' every 5 seconds.
    sensor_task = FileSensor( task_id= "archive_sensor", poke_interval= 5, filepath = '/opt/airflow/Files/Files.zip' )
    
    #Unarchive Files.zip' once available
    unarchive_task = PythonOperator(
        task_id = 'unarchive_files',
        python_callable= unarchive
    )

    #create and store the merged dataframe to be used downstreem
    merge_task = [
        PythonOperator(
            task_id = f'Create_merge_datasets_{i+1}',
            python_callable = get_avg_csv,
            op_kwargs = {'years' : split_years_list[i]}
    ) for i in range(6)]

    #Simply waits for the upstream parallel tasks to finish. Doesn't do anything.
    merge_wait_task = DummyOperator(task_id='wait_task')
    
    #Create the images in parallel and store them in the images folder
    create_images_task = [
        PythonOperator(
            task_id = f'Create_images_{i+1}',
            python_callable = map_maker,
            op_kwargs = {'years' : split_years_list[i]}
    ) for i in range(6)]
    
    #delete the extracted Archive
    delete_task = PythonOperator(
        task_id = f'delete_archive',
        python_callable = del_archive
    )

sensor_task >> unarchive_task >> merge_task >> merge_wait_task >> create_images_task >> delete_task