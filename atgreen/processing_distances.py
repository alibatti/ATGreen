#Import standard libraries needed for the Data Processing and Cleaning
from .basic import *
from rtree import index
from geopy.distance import geodesic
import subprocess
import datetime


"""             Distance Calculation using OSRM                          """

def osrm_files_creation(filename, folder_input, folder_working, profile):
    
    """ 
    Create files to run OSRM.
    The function move files in the right folder for the computation of the osrm graph (osrm-extraction) and its subsequent contraction.
    The adopted approach follows the guidelines here: https://gis-ops.com/osrm-nodejs-bindings/

    
    -------------------------------------------------------  
    
    Parameters:
    filename: osm.pbf filename
    folder_input: folder where the input file is stored
    folder_working: working folder where files are created
    profile: profile for the routing
    
    -------------------------------------------------------  
    
    Return:
    Diagnostic code
    
    """
    os.chdir(folder_working)
    file_from=f"{folder_input}/{filename}.osm.pbf"
    file_to=f"./{filename}.osm.pbf"
    res=subprocess.run(f"cp {file_from} {file_to}", shell=True)
    if res.returncode==0:
        #Generate extract with correct profile 
        cmd=f"node_modules/osrm/lib/binding/osrm-extract {filename}.osm.pbf -p node_modules/osrm/profiles/{profile}.lua"
        res=subprocess.run(cmd, shell=True)
        if res.returncode==0:
        #Generate contraction
            cmd=f"node_modules/osrm/lib/binding/osrm-contract {filename}.osrm"
            res=subprocess.run(cmd, shell=True)
            return res
        else:
            return res
    else:
        return res
        
def coords_vector_identification(df, len_vector, filename, folder_working):
    
    """ 
    The function generate a list of coordinates x y. The coordinate list are prioritize so that coordinates that appear more frequently in our distance matrix are computed first. 
    
    -------------------------------------------------------  
    Parameters:
    
    df: df with pairs of coordinates to compute distances. 
            Notice that, as we use the foot profile, the distance between A to B is assumed to be equal to the distance between B to A. This saves computational resources. 
    len_vector: len of list of coordinates to include in the distance array computation. Notice that computing the entire distance matrix at once is not feasible for most of the cities. 
    filename: name of the file where the list of coordinates is saved. 
    
    -------------------------------------------------------  
    
    Description:
    
    Step 1: Identify origin-destination distances not yet computed. Sort origins by frequnecy. 
    Step 2: Iteratively: append the coordinates of the more frequent origin and the coordinates of all destinations associated with this origin. 
            Repeat until you reach the desired length of the vector.
    Step 3: Save the coordinates list to txt file
    
    -------------------------------------------------------  

    Return: 
    pandas.dataframe 
    """

    #Step 1:
    tmp=df[(df['walk_durations'].isnull()==True) & (df['walk_access']==1)].reset_index(drop=True)
    tmp1=tmp[['long_source','lat_source','x_dest', 'y_dest']].groupby(['long_source','lat_source']).count().sort_values(by=['x_dest', 'y_dest'], ascending=False).reset_index().drop(columns=['x_dest', 'y_dest']).rename(columns={'long_source':'long','lat_source':'lat'})

    #Step 2:
    subset=pd.DataFrame({'long':[], 'lat':[]})
    i=0
    while (len(subset)<len_vector) and (i in list(tmp1.index)):
        subset=pd.concat([subset, tmp1[(tmp1.index==i)][['lat', 'long']]])
        subset=pd.concat([subset, tmp[((tmp['lat_source']==tmp1.at[i,'lat']) & (tmp['long_source']==tmp1.at[i,'long']))][['lat_dest','long_dest']].rename(columns={'long_dest':'long', 'lat_dest':'lat'})])
        if len(subset)>2:
            subset=subset.drop_duplicates()
        i=i+1        

    #Step 3:
    subset=subset.reset_index(drop=True).reset_index()
    os.chdir(folder_working)
    np.savetxt(filename, subset[['long','lat']].values, delimiter=',', header='long,lat') 
    return subset
                       

def one_run_osrm(filename_osm, filename_input, filename_output, working_folder):
    
    """
    Run OSRM - see code in run.js
    
    -------------------------------------------------------  
    
    Parameters:
    
    filename_osm: name of osm.pbf original file
    filename_input: name of input file 
    filename_output: name of output file 
    working_folder: working_folder
    
    -------------------------------------------------------  
    
    Return
    Diagnostic code
    
    """
    os.chdir(working_folder)
    cmd=f"node run.js {filename_input} {filename_osm} {filename_output}"
    res=subprocess.run(cmd, shell=True)   
    return res

    
def merge_one_run(df, subset,filename_input, working_folder):
    
    """ 
    Assemble information from the various OSRM runs
    
    -------------------------------------------------------
    
    Parameters:
    df: list of distances queries provided as a pandas.DataFrame
    subset: list of coordinates, as submitted to osrm (from coords_vector_identification)
    city: name of the city
    filename_input: name of output file from one_run_osrm
    working_folder: working_folder
    
    -------------------------------------------------------  

    Description:
    Step 1: Read computed durations.
    Step 2: Merge df and subset to identify position of each origin or destination from subset in df.
    Step 3: Merge the durations to the df based on the computed positions and update information on the duration (from column value).
            Repeat swopping origin and destination position (for the foot profile the distance is the same)
            Keep only relevant columns:
            [0: origin cell id
             1: destination cell id
             2,3: x,y coord of origin
             4,5: x,y coord of destination
             'durations': walking distance in seconds]
             
    -------------------------------------------------------  

    Return: 
    pandas.DataFrame
    """

    #Step 1:
    os.chdir(working_folder)
    durations = pd.read_csv(filename_input, delimiter=',', header=None, low_memory=False).reset_index().melt(id_vars='index').rename(columns={'index':'pos1','variable':'pos2' })
    #Step 2:
    df=pd.merge(df, subset,left_on=['lat_source','long_source'],  right_on=['lat','long'], how='left').rename(columns={'index':'pos1'})
    df=pd.merge(df, subset, left_on=['lat_dest','long_dest'], right_on=['lat','long'], how='left').rename(columns={'index':'pos2'})
    df=df[['x_source', 'y_source', 'lat_source', 'long_source','x_dest', 'y_dest', 'lat_dest', 'long_dest', 'pos1', 'pos2', 'walk_durations', 'walk_access_source', 'walk_access_dest', 'walk_access']]
    #Step 3: 
    df=pd.merge(df, durations, on=['pos1', 'pos2'], how='left')
    df['walk_durations']=df['walk_durations'].fillna(df['value'])
    df=df.drop(columns=['value'])
    df=pd.merge(df, durations, left_on=['pos2', 'pos1'], right_on=['pos1', 'pos2'], how='left')
    df['walk_durations']=df['walk_durations'].fillna(df['value'])
    df=df.drop(columns=['value'])
    df=df[['x_source', 'y_source', 'lat_source', 'long_source','x_dest', 'y_dest', 'lat_dest', 'long_dest', 'walk_durations', 'walk_access_source', 'walk_access_dest', 'walk_access']]
    return df

def osrm_files_deletion(filename_osm, filename_coords, filename_dur , working_folder):
    
    """ 
    Delete files created to compute distances
    
    -------------------------------------------------------  
    Parameters:
    
    city: name of the city
    profile: profile for the routing
    city_country_ISO: dict with city to ISO code mapping
    
    -------------------------------------------------------  
    
    Return: 
    empty
    """
    os.chdir(working_folder)
    #Remove copied osm.pbf file
    cmd=f"rm {filename_osm}.osm.pbf"
    res=subprocess.run(cmd, shell=True)
    if res.returncode!=0:
        raise Exception('Unable to delete file.')
    #Remove osrm files
    cmd=f"rm {filename_osm}.osrm*"
    res=subprocess.run(cmd, shell=True)
    if res.returncode!=0:
        raise Exception('Unable to delete file.')
    #Remove other ancillary files
    cmd=f"rm {filename_coords}"
    res=subprocess.run(cmd, shell=True)
    if res.returncode!=0:
        raise Exception('Unable to delete file.')
    cmd=f"rm {filename_dur}"
    res=subprocess.run(cmd, shell=True) 
    if res.returncode!=0:
        raise Exception('Unable to delete file.')
    
