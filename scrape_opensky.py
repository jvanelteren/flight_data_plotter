#%%
import urllib
from pathlib import Path
import datetime
import tarfile
import gzip
import pandas as pd
import pickle

#%%
base_url = Path(r'opensky-network.org/datasets/states/')
save_dir = Path(r'C:/Users/Jesse/Documents/GitHub/projects/flight_data_plotter/opensky_scrape')
to_download = datetime.datetime.strptime('2020-04-13','%Y-%m-%d')
inc = datetime.timedelta(7)
#skipped 01-13 because not all hours are there

box_nl= (50.74, 53.57, 3.24, 7.24)
min_lat, max_lat, min_lon, max_lon = box_nl

def path_to_url(path):
    return 'https://'+str(path.as_posix())

def download_data(url, filename):
    print(url)
    urllib.request.urlretrieve(url, filename)
    print('go')

def untar_data(path):
    tar = tarfile.open(path)
    tar.extractall(save_dir)
    tar.close()

def select_nl(df):
    return df.loc[(min_lon < df['lon']) & (df['lon'] < max_lon) & 
                (min_lat < df['lat']) &  (df['lat'] < max_lat) & 
                ~df['onground']]

while to_download < datetime.datetime.now():
    for hour in range(19,24):
        print('starting', to_download, hour)
        url_dir = base_url / to_download.strftime('%Y-%m-%d') / str(hour).zfill(2) 
        filename = 'states_' + to_download.strftime('%Y-%m-%d') + '-' + str(hour).zfill(2) + '.csv.tar'
        gzip_filename = 'states_' + to_download.strftime('%Y-%m-%d') + '-' + str(hour).zfill(2) + '.csv.gz'
        pickle_filename = to_download.strftime('%Y-%m-%d') + '-' + str(hour).zfill(2) + '.pickle'
        
        download_data(path_to_url(url_dir/filename), save_dir/filename)
        untar_data(save_dir/filename)
        df = pd.read_csv(gzip.open(save_dir/gzip_filename, 'rb'))
        with open(save_dir/pickle_filename, 'wb') as handle:
            pickle.dump(select_nl(df), handle, protocol=pickle.HIGHEST_PROTOCOL)
        (save_dir/filename).unlink()
        (save_dir/gzip_filename).unlink()
    to_download += inc

# %%