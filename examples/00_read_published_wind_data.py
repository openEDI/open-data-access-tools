import os
import sys
import glob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
mpl.rcParams['lines.markersize'] = 2



#%% Functions

def get_file_structure(file_path, resolution, year, month, day):
    try:
        # Create the directory structure
        directory = os.path.join(file_path, 'resolution=' + resolution + "/year="+year+"/month="+month+"/day="+day+"/")
        
        return directory

    except Exception as e:
        print(f"Error: {e}")
        
        
        
        
#%% Definitions


# Define date to read (or wildcard with *)
year   = '2022'
month  = '06'
day    = '20'


# Read data at this resolution (1min or 20Hz)
resolution = '1min'


# File path
data_path = 'Y:\Wind-data/Restricted/Projects/NSO/Data_publish/NSO/'






#%% Read data



## Wind data


# Get a list of available data files at specified date(s)
path = get_file_structure(file_path = data_path+'met_masts/inflow_mast/', resolution = resolution, year=year, month=month, day=day)
inflow_files = sorted(glob.glob(path +'Inflow_Mast_'+resolution+'_' + year + '-' + month + '-' + day + '_' + '*.parquet'))  

path = get_file_structure(file_path = data_path+'met_masts/wake_masts/', resolution = resolution, year=year, month=month, day=day)
mast_files = sorted(glob.glob(path +'Wake_Masts_'+resolution+'_' + year + '-' + month + '-' + day + '_' + '*.parquet'))   #     


if len(inflow_files) == 0:
    print ("No data files available.")
    sys.exit()

   
# Read all data sets 
inflow = pd.DataFrame()    
for datafile in inflow_files:
    inflow = pd.concat( [inflow, pd.read_parquet(datafile)]) 

masts = pd.DataFrame()
for datafile in mast_files:
    masts = pd.concat( [masts, pd.read_parquet(datafile)] ) 
    

    

    
#%% Plot Timeseries


fig = plt.figure(figsize=(15,9))   
plt.suptitle(year + month + day)
ax1 = plt.subplot(5, 2, 1)
ax1.set_ylabel('Temperature  ($^\circ$C)')


ax2 = plt.subplot(5, 2, 2)
ax2.set_ylabel('RH (%)')

ax3 = plt.subplot(5, 2, 3)
ax3.set_ylabel('Stability R_f')
ax3.set_ylim(-0.2,0.2)

ax4 = plt.subplot(5, 2, 4)
ax4.set_ylabel('Heat flux (W m$^{-2}$)')

ax5 = plt.subplot(5, 2, 5)
ax5.set_ylabel('Wind speed 7m (m s$^{-1}$)')

ax6 = plt.subplot(5, 2, 6)
ax6.set_ylabel('Wind direction 7m ($^\circ$)')

ax7 = plt.subplot(5, 2, 9)
ax7.set_ylabel('length scale $w$ (m)')

ax8 = plt.subplot(5, 2, 10)
ax8.set_ylabel('length scale $U$ (m)')

ax9 = plt.subplot(5, 2, 7)
ax9.set_ylabel('TI 7m')

ax10 = plt.subplot(5, 2, 8)
ax10.set_ylabel('TKE 7m (m$^{2}$ s$^{-2}$)')
                   
ax1.plot(  inflow.Temp_2m  ,"." , label = "Tem 2m")
try:
    ax1.plot(inflow.Temp_3m,'8',label = 'Temp 3.5m', color="C0")
    ax1.plot(inflow.Temp_7m,'s',label = 'Temp 7m', color="C0")   
except:
    pass
ax2.plot(  inflow.RH  ,"." )
ax3.plot(  inflow.R_f  ,"." )
ax4.plot(  inflow.H_S  ,"." )    
ax5.plot(  inflow.wspd_7m  ,".",label='inflow' )
ax6.plot(  inflow.wdir_7m,"." )
ax7.plot(  inflow.ls_w_7m  ,"." )    
ax8.plot(  inflow.ls_U_7m  ,"." )
ax9.plot(  inflow.TI_U_7m  ,"." )    
ax10.plot(  inflow.TKE_7m  ,"." )   

if len(masts) !=0: 
    ax5.plot(  masts.m1_wspd_7m  ,"." ,label='mast 1')
    ax5.plot(  masts.m2_wspd_7m  ,"." ,label='mast 2')
    ax5.plot(  masts.m3_wspd_7m  ,"." ,label='mast 3')                    
    ax6.plot(  masts.m1_wdir_7m  ,"." )
    ax6.plot(  masts.m2_wdir_7m  ,"." )
    ax6.plot(  masts.m3_wdir_7m  ,"." )  
    ax10.plot(  masts.m1_TKE_7m  ,"." )
    ax10.plot(  masts.m2_TKE_7m  ,"." )
    ax10.plot(  masts.m3_TKE_7m  ,"." )  
    ax9.plot(  masts.m1_TI_U_7m  ,"." )
    ax9.plot(  masts.m2_TI_U_7m  ,"." )
    ax9.plot(  masts.m3_TI_U_7m  ,"." )                  

ax5.legend(loc=3, markerscale=3)
ax1.legend(markerscale=3)             
for ax in [ax1, ax2, ax3, ax4, ax5, ax6, ax7, ax8, ax9, ax10]:
    ax.grid(True)

plt.tight_layout()  








 
        
        
        
  