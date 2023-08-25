import os
import sys
import glob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
mpl.rcParams['lines.markersize'] = 2



#%% Functions

def get_file_structure(file_path, year, month, day):
    try:
        # Create the directory structure
        directory = os.path.join(file_path, "resolution=*" + "/year="+year+"/month="+month+"/day="+day+"/")
        
        return directory

    except Exception as e:
        print(f"Error: {e}")
        
        
        
        
#%% Definitions


# Define date and hour to read (or wildcard with *)
year   = '2023'
month  = '04'
day    = '02'
hour   = '21'



# File path
data_path = 'Y:\Wind-data/Restricted/Projects/NSO/Data_publish/NSO/'



#%% Read Lidar data


# Get a list of available data files at specified date(s)
path = get_file_structure(file_path = data_path+'lidar/', year=year, month=month, day=day)
lidar_files = sorted(glob.glob(path +'Lidar_' + year + '-' + month + '-' + day + '_' + hour + '*.parquet'))  

if len(lidar_files) == 0:
    print ("No data files available.")
    sys.exit()

   
# Read all data sets 
lidar = pd.DataFrame()    
for datafile in lidar_files:
    lidar = pd.concat( [lidar, pd.read_parquet(datafile)]) 

    

    
#%% Plot  Dependence on Azimuth and time series


try:
    fig = plt.figure()   
    plt.suptitle(year + month + day)
    ax1 = plt.subplot(1, 1, 1)
    ax1.set_ylabel('Doppler velocity (m/s)')
    ax1.set_xlabel('Azimuth (deg)')
    p = ax1.scatter(lidar.Az, lidar.Doppler_filtered,c=lidar.index, alpha=0.5)
    N_TICKS = 9
    indexes = [lidar.index[i] for i in np.linspace(0,lidar.shape[0]-1,N_TICKS).astype("int64")] 
    cb = plt.colorbar(p, orientation='vertical',
                      ticks= lidar[lidar.Range_gate==0].loc[indexes].index.astype("int64"))  
    cb.ax.set_yticklabels([index.strftime('%d %b %Y %H:%M') for index in indexes])
    ax1.grid(True)
    plt.tight_layout()
    
    
    fig = plt.figure()   
    plt.suptitle(year + month + day)
    ax1 = plt.subplot(1, 1, 1)
    ax1.set_xlabel('Doppler velocity (m/s)')
    p = ax1.plot(lidar.Doppler_filtered)
    ax1.grid(True)
    plt.tight_layout()
except:
    pass







 
        
        
        
  
