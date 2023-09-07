#Import standard libraries needed for the Data Processing and Cleaning
from .basic import *
from .utils_psql import *


def accessibility_index_pipeline(city: str, index_params: dict, index_storage_name: str, db_params: dict, min_intersection):
    # Step 1: Load required data
    # Population grid
    grid=query4grid(city, db_params)
    n_rows=query4filteredtable('cities_boundary', 'public', db_params, 'city', city).reset_index()['n_rows'][0]
    grid['id']=grid.apply(lambda x: x['y']+ n_rows*(x['x']-1), axis=1)
                
    # Green remapped grid from correct data sources
    if index_params['source'] not in ['OSM', 'ESA']:
        raise Exception("Value for the parameter 'source' should be in ['OSM', 'ESA']")
    if index_params['source']=='OSM':
        
        engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")
        sql =f"""SELECT * FROM osm.osm_greencombinations """ 
        df=pd.read_sql(sql,engine)
        engine.dispose()
        prefix=df[df['value']==index_params['green_type']]['key'].values[0]
        green_on_grid=queryRemappedGreen(city,"osm.osm2grid", f"{str(prefix)}_", index_params['min_park_size'], min_intersection, db_params)
        
    else:
        green_on_grid=queryRemappedGreen(city,"esa.esa2grid", f"0_", index_params['min_park_size'], min_intersection, db_params)
       
    # Distances
    #Get distances
    if index_params['distances'] not in ['street-network', 'geodesic']:
        raise Exception("Value for the parameter 'distances' should be in ['street-network', 'geodesic']")
    distances=queryDistances(city , index_params['distances'], db_params)
    distances['dist']=distances['dist']/10
    
    # Compute index
    if index_params['index'] not in ['minimum_distance', 'exposure', 'per_person']:
        raise Exception("Value for the parameter 'index' should be in ['minimum_distance', 'exposure', 'per_person]")
    if index_params['index']=='minimum_distance':
        index=minimum_distance_index(grid, green_on_grid, distances, index_storage_name)
        
    elif index_params['index']=='exposure':
        index=exposure_index(grid, green_on_grid, distances, index_params['time_threshold'], index_storage_name)
        
    else:
        cells_unmasked=query4grid_unmasked(city, db_params)
        index=per_person_index(grid, cells_unmasked, green_on_grid, distances, index_params['time_threshold'], index_storage_name, n_rows)

    #Merge with grid:
    grid=pd.merge(grid, index, how='left', on=['id'])
    # if index is per person or exposure, the absence of green in the surrounding area means that the exposure or the index per person is 0
    if index_params['index']=='minimum_distance':
        grid[index_storage_name]=grid[index_storage_name].fillna(-2)
    else:
        grid[index_storage_name]=grid[index_storage_name].fillna(0)
    grid.loc[grid['inbound']==0, index_storage_name]=-2
    grid.loc[grid['population']==0, index_storage_name]=-2
    
    # Compute additional info, along with index
    # on whether the area satisfy the target:
    grid[f'TargetSatisfied_{index_storage_name}']=-2
    if index_params['index']=='minimum_distance':
        grid.loc[(grid[index_storage_name]>index_params['time_threshold']) & (grid['inbound']==1) & (grid['population']>0) & (grid[index_storage_name]!=-2), f'TargetSatisfied_{index_storage_name}']=0
        grid.loc[(grid[index_storage_name]<=index_params['time_threshold']) & (grid['inbound']==1) & (grid['population']>0) & (grid[index_storage_name]!=-2), f'TargetSatisfied_{index_storage_name}']=1
    else :
        grid.loc[(grid[index_storage_name]<index_params['exposure_target']) & (grid['inbound']==1) & (grid['population']>0) & (grid[index_storage_name]!=-2), f'TargetSatisfied_{index_storage_name}']=0
        grid.loc[(grid[index_storage_name]>=index_params['exposure_target']) & (grid['inbound']==1) & (grid['population']>0) & (grid[index_storage_name]!=-2), f'TargetSatisfied_{index_storage_name}']=1

    # people in this cell have a better index than x% of population in the city boundary:
    if  index_params['index']=='minimum_distance':
        grid['tmp']=grid[index_storage_name]
        grid.loc[(grid[index_storage_name]==-2) & (grid['population']>0), 'tmp']=grid[index_storage_name].max()+1 #Account for cases with positive population but nothing nearby - set index to -2 normally but here would create problem with the sorting, so replaced with max index value + 1
        tmp=grid[['tmp','population']].groupby('tmp').sum().reset_index()
        tmp=tmp.sort_values(by='tmp', ascending=False)
        tmp['pop_cum']=tmp['population'].cumsum()
        tmp[f'BetterThanEqual_{index_storage_name}']=tmp['pop_cum']/tmp['population'].sum()
        grid=pd.merge(grid, tmp[[f'BetterThanEqual_{index_storage_name}', 'tmp']], on='tmp', how='left')
        grid.loc[grid['population']==0, f'BetterThanEqual_{index_storage_name}']=-2

        grid.loc[grid['population']==0, f'BetterThanEqual_{index_storage_name}']=-2
    else:
        tmp=grid[[f'{index_storage_name}','population']].groupby(f'{index_storage_name}').sum().reset_index()
        tmp=tmp.sort_values(by=index_storage_name, ascending=True)
        tmp['pop_cum']=tmp['population'].cumsum()
        tmp[f'BetterThanEqual_{index_storage_name}']=tmp['pop_cum']/tmp['population'].sum()
        grid=pd.merge(grid, tmp[[f'BetterThanEqual_{index_storage_name}', f'{index_storage_name}']], on=f'{index_storage_name}', how='left')
        grid.loc[grid['population']==0, f'BetterThanEqual_{index_storage_name}']=-2
    
    return grid[['id',  index_storage_name, f'BetterThanEqual_{index_storage_name}', f'TargetSatisfied_{index_storage_name}' ]]

def per_person_index(grid, grid_unmasked, green_grid, distances, threshold, index_storage_name, n_rows):
    
    tmp=distances.copy()
    tmp_grid=grid.copy()
    #Augment grid information with info from population in boundaries to ensure we are able to identify concurrent population also from boundaries 
    grid_unmasked.loc[grid_unmasked['population']==-200, 'population']=0
    grid_unmasked['id']=grid_unmasked.apply(lambda x: x['y']+ n_rows*(x['x']-1), axis=1)
    tmp_grid=pd.merge(tmp_grid[['id','inbound']], grid_unmasked[['id','population']], on=['id'], how='left')
    
    #Create tuple index from combination of x and y
    tmp=tmp[tmp['source'].isin(tmp_grid[tmp_grid['population']>=0]['id'])]
    tmp=tmp[tmp['dest'].isin(green_grid[green_grid['green']==1]['id'])]
    tmp['id_tmp']=tmp[['dest']]
    #Filter out cells not reachable
    tmp=tmp[tmp['dist'].isnull()==False]
    tmp=tmp[tmp['dist']<=threshold]
    #Make a copy
    tmp_1=tmp.copy() #Copy of distances
    
    #First identify size reachable as concurrent
    green_grid=green_grid[['id', 'si']].groupby(['id']).sum()
    tmp_1=pd.merge(tmp_1,tmp_grid[['id', 'population']], how='left', left_on=['source'], right_on=['id'])
    tmp_1=pd.merge(tmp_1,green_grid, how='left', left_on=['dest'], right_on=['id'])
    tmp_11=tmp_1[['source', 'si']].groupby(['source']).sum().reset_index().rename(columns={'si':'si_tot'})
    tmp_1=pd.merge(tmp_1,tmp_11, how='left', on=['source'])
    tmp_1['pop_on_dest']=np.ceil(tmp_1['population']*(tmp_1['si']/tmp_1['si_tot'])) #round to the ceiling integer to avoid cases where the available green is more than the size of the cell - equivalent to set that the maximum green per person in the cell is equal to the total green in the cell
    tmp_1=tmp_1[tmp_1['pop_on_dest']>0]
    tmp_1=tmp_1[['dest','pop_on_dest']].groupby(['dest']).sum().reset_index()
    del [tmp_11]
    
    #Green per person in cell
    green_perperson=pd.merge(tmp_1,green_grid, how='left', left_on=['dest'], right_on=['id'])
    del [tmp_1]
    green_perperson['si_perperson']=green_perperson['si']/green_perperson['pop_on_dest']*10000 #in mq2
    green_perperson=green_perperson[['dest','si_perperson']]
    
    #Get green per person available by source location summing over all green per person reachable
    tmp=pd.merge(tmp, green_perperson, on=['dest'], how='left')
    del [green_perperson]
    tmp.fillna(0)
    index=tmp[['source', 'si_perperson']].groupby(['source']).sum()
    del [tmp]
    index=index.reset_index().rename(columns={'source':'id', 'si_perperson':index_storage_name})
    index=index[index[['id']].isin(tmp_grid[tmp_grid['inbound']==1]['id'])]   
    return index[['id',index_storage_name]]


def minimum_distance_index(grid, green_grid, distances, index_storage_name):
    tmp=distances.copy()
    tmp_grid=grid.copy()

    #keep only cells within the bound as sources
    tmp=tmp[tmp['source'].isin(tmp_grid[tmp_grid['inbound']==1]['id'])]
    tmp=tmp[tmp['dest'].isin(green_grid[green_grid['green']==1]['id'])]
    #Filter out cells not reachable
    tmp=tmp[tmp['dist'].isnull()==False]
    #Compute index taking the minimum distance of all cells not filtered out for each source cell
    index=tmp[['source', 'dist']].groupby(['source']).min().reset_index().rename(columns={'source':'id','dist':index_storage_name})
    return index
      
def exposure_index(grid, green_grid, distances, threshold, index_storage_name):

    tmp=distances.copy()
    tmp_grid=grid.copy()
    #keep only cells within the bound as sources
    tmp=tmp[tmp['source'].isin(tmp_grid[tmp_grid['inbound']==1]['id'])]
    tmp=tmp[tmp['dest'].isin(green_grid[green_grid['green']==1]['id'])]
    #Filter out cells not reachable
    tmp=tmp[tmp['dist'].isnull()==False]
    tmp=tmp[tmp['dist']<=threshold]
    
    tmp=pd.merge(tmp,green_grid, how='left', left_on=['dest'], right_on=['id'])
    tmp.fillna(0)
    index=tmp[['source', 'si']].groupby(['source']).sum().reset_index().rename(columns={'source':'id', 'si':index_storage_name})
    return index
     
