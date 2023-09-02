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
        directory = os.path.join(file_path,  "year="+year+"/month="+month+"/day="+day+"/")
        
        return directory

    except Exception as e:
        print(f"Error: {e}")
        
        
        
#%% Definitions


# Define date to read (or wildcard with *)
year   = '*'
month  = '*'
day    = '*'

year   = '2022'
month  = '11'
day    = '*'



# File path
data_path = 'Y:\Wind-data/Restricted/Projects/NSO/Data_publish/NSO/'


# Read data at this resolution (1min or 20Hz)
resolution = '1min'


# If plot==1: Make an overview plot together with wind data
plot=1



#%% Read data


# Get a list of available loads data files at specified date(s)
path = get_file_structure(file_path = data_path+'loads_{}/'.format(resolution), year=year, month=month, day=day)
loads_files = sorted(glob.glob(path +'Loads_'+resolution+'_'  + year + '-' + month + '-' + day + '_*.parquet'))  

if len(loads_files) == 0:
    print ("No data files available.")
    sys.exit()

   
# Read all loads data sets 
loads = pd.DataFrame()    
for datafile in loads_files:
    loads = pd.concat( [loads, pd.read_parquet(datafile)]) 
    
    
# ### Plot Histogram for loads
# hist_columns = [col for col in loads.columns if ('An' in col)  & ('_m' not in col) & ('_s' not in col)]
# loads[hist_columns].select_dtypes('number').hist(figsize=(14,9), bins=300, density=True)
# plt.tight_layout()
# plt.show()

    

    
#%% Overview combined with winds


if plot == 1:
    
    # Read winds
    resolution_winds = '1min'   # one out of: '20Hz', '1min'

    path = get_file_structure(file_path = data_path+'inflow_mast_{}/'.format(resolution_winds), year=year, month=month, day=day)
    inflow_files = sorted(glob.glob(path +'Inflow_Mast_'+resolution+'_' + year + '-' + month + '-' + day + '_' + '*.parquet'))  


    inflow = pd.DataFrame()    
    for datafile in inflow_files:
        inflow = pd.concat( [inflow, pd.read_parquet(datafile)]) 
         
    inflow = inflow[loads.index[0]:loads.index[-1]]    
    
    ## Combine loads and wind
    all_data = pd.merge(loads, inflow, left_index=True, right_index=True, how="outer")
    
    
    mpl.rcParams['lines.markersize'] = 1
   
 
    ## Plot time series
    fig = plt.figure(figsize=(17,9))   

    ax1 = plt.subplot(4, 2, 1)
    ax1.set_ylabel('Wind speed 3m (m s$^{-1}$)')    

    ax6 = plt.subplot(4, 2, 3, sharex=ax1)
    ax6.set_ylabel('Wind direction 3m ($^\circ$)')
    plt.yticks([0, 90, 180, 270, 360], ['N', 'E', 'S', "w", "N"]) 

    ax9 = plt.subplot(4, 2, 4, sharex=ax1)
    ax9.set_ylabel('TI 3m')
    
    ax10 = plt.subplot(4, 2, 2, sharex=ax1)
    ax10.set_ylabel('TKE 3m (m$^{2}$ s$^{-2}$)')
    
    ax7 = plt.subplot(4, 2, 7, sharex=ax1)
    ax7.set_ylabel('Bending moment SO (kNm)')
    
    ax8 = plt.subplot(4, 2, 8, sharex=ax1)
    ax8.set_ylabel('Torque moment DO (kNm)')    

    ax4 = plt.subplot(4, 2, 6, sharex=ax1)
    ax4.set_ylabel('Displacement (mm)')    

    ax3 = plt.subplot(4, 2, 5, sharex=ax1)
    ax3.set_ylabel('Tilt ($^\circ$)')

    try:
        ax1.plot(inflow.wspd_3m,".",color='black', label = "inflow")
        ax6.plot(inflow.wdir_3m,".",color='black')
        ax9.plot(inflow.TI_U_3m,".",color='black')    
        ax10.plot(inflow.TKE_3m,".",color='black')  
    except:
        pass

    ax1.legend(fontsize=7, markerscale= 4, loc='center left', bbox_to_anchor=(1, 0.5))
    for column in loads[[col for col in loads.columns if ('SO_Bending' in col)  & ('_m' not in col) & ('_s' not in col)]]:
        ax7.plot(loads[column],".", label = loads[column].name[:2]) 
    ax7.legend(fontsize=7, markerscale= 4, loc='center left', bbox_to_anchor=(1, 0.5))
    for column in loads[[col for col in loads.columns if ('Torque' in col)  & ('_m' not in col) & ('_s' not in col)]]:
        ax8.plot(loads[column],".", label = loads[column].name[:2]) 
    ax8.legend(fontsize=7, markerscale= 4, loc='center left', bbox_to_anchor=(1, 0.5))
    for column in loads[[col for col in loads.columns if ('Disp' in col)  & ('_m' not in col) & ('_s' not in col)& ('_raw' not in col)& ('_level' not in col)]]:
        ax4.plot(loads[column],".", label = loads[column].name.replace('_Disp_', ' ')) 
    ax4.legend(fontsize=7, markerscale= 4, loc='center left', bbox_to_anchor=(1, 0.5))
    ax3.plot(loads.projected_sun_angle.where((loads.projected_sun_angle<90) & (loads.projected_sun_angle>-90)), ".", color='black', label="nom")
    for column in loads[[col for col in loads.columns if ('Tilt' in col)  & ('_m' not in col) & ('_s' not in col) & ('SO' in col)& ('_raw' not in col)]]:
        ax3.plot(loads[column],".", label = loads[column].name[:2]) 
    ax3.legend(fontsize=7, markerscale= 4, loc='center left', bbox_to_anchor=(1, 0.5))
    
    for ax in [ax1, ax3, ax4, ax6, ax7, ax8, ax9, ax10]:
        ax.grid(True)
        ax.set_xlim(loads.index[0], loads.index[-1])

    ax8.set_xlabel('Date/ Time (UTC)')
    ax7.set_xlabel('Date/ Time (UTC)')

    fig.autofmt_xdate() 
    plt.tight_layout() 


 

        
        
  
