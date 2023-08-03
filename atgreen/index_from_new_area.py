from atg import *

def indexFromPolygon(geometry:shapely.geometry, continent:str, buffer:float, index_params:dict, db_params:dict, tables_dict:dict, schemas_dict:dict, directories_dict:dict, name_area:str='myArea', keep:bool=False, validation:bool=True, profile:str='foot', steps_to_run:list=range(8)):
    
    """
    Pipeline to add new area to the database and compute accessibility index
    
    -------------------------------------------------------   
    
    Parameters:
    
    geometry: geometry of the new area for the study. It should be provided in EPSG:4326 / WGS84.
    continent: continent of the new area, according to OSM continent split from geofabrik: https://download.geofabrik.de/. 
    buffer: size of the buffer to be constructed around the study area to avoid boundary issues. To be provided in meters.
    index_params: parameters of the index to be computed. The parameters have to be provided as a dictionary with keys: 
            'name'
            'index'
            'min_park_size'
            'time_threshold'
            'green_type'
            'source'
            'distances'
            'exposure_target'
    db_params: database information to establish connection.  It must include the following keys:
            'db_host': host of the database
            'db_user': username
            'db_password': password
            'db_port': port to connect to the database
            'db_name': name of the database
    tables_dict: dictionary with tables, as from the structure of the database. It must include the following keys:
            'table_cities_boundary': 
            'table_population': 
            'table_index': 
            'table_osm2grid': 
            'table_osmconfounders2grid':
            'table_esa2grid':
            'table_esa': 
    schemas_dict: dictionary with schemas, as from the structure of the database. It must include the following keys:
            'cities_boundary':
            'population': 
            'esa': 
            'osm':
            'distances':
            'index':
    directories_dict: dictionary with saving directories. All directories to be provided without the final /. It must include the following keys:
            'database_directory':
            'osm_directory':
            'distances_directory':
            'dictionaries_directory':
            'osrm_directory':
            'working_directory':
    name_area: name of the new area to be used in the database
    keep: boolean for whether the new file in the database should be kept at the end of the computation or not [whether a permanent addition to the DB or not]
    validation: boolean for whether to generate files for validation of the green areas or not
    profile: mobility profile, as from standard profiles of OpenSourceRoutingMachine (https://project-osrm.org/), Defalut: 'foot'
    steps_to_run: list of steps to be run
    
    -------------------------------------------------------   
    
    Return:
    empty
    
    """
    
    """Check that the choose name for the area is not already in use in the db"""
    listOfAreas=getListOfAreas(db_params)
    if name_area in listOfAreas: 
        raise Exception("Error. The chosen name is already in use in the database. Specify a different name for the new area.")

    """Check that inputs are correctly specified:"""
    ValidList=['africa', 'asia', 'europe','north-america', 'australia-oceania', 'south-america', 'central-america']
    if continent not in ValidList:
        raise Exception("Error. Specified continent is invalid. Valid values are: ['africa', 'asia', 'europe','north-america', 'australia-oceania', 'south-america', 'central-america']")
 
    ValidList=['name','index', 'min_park_size', 'time_threshold', 'green_type', 'source', 'distances', 'exposure_target']
    for k in index_params.keys():
        if k not in ValidList:
            raise Exception(f"Error. Specified index_param key {k} is invalid. Valid values are: ['index', 'min_park_size', 'time_threshold', 'green_type', 'source', 'distances', 'exposure_target']")

    """ Step 01: Append geometry to list of cities """
    if 1 in steps_to_run:
        print('Step 01: Append geometry to list of cities')
        geomToCitiesBoundary(geometry, name_area, tables_dict['table_cities_boundary'], schemas_dict['cities_boundary'], db_params) 
    geometry_enlarged=query4cityboundary(name_area, db_params, buffer, tables_dict['table_cities_boundary']).geometry 


    """ Step 02: Generate raster for population """
    if 2 in steps_to_run:
        print('Step 02: Generate raster for population')
        file='/mnt/work/accessToGreen/data/raw_data/ghs_pop/ghs_pop_global/GHS_POP_E2015_GLOBE_R2019A_4326_9ss_V1_0_1.tif'
        geomToPop(geometry_enlarged, name_area, directories_dict['working_directory'], directories_dict['database_directory'], tables_dict['table_population'], db_params, file)


    """ Step 03: Generate OSM files """
    if 3 in steps_to_run:
        print('Step 03: Generate OSM files')
        geomToOSM(geometry_enlarged, continent, db_params, directories_dict['working_directory'], directories_dict['osm_directory'], directories_dict['dictionaries_directory'], name_area, schemas_dict['osm'], validation)
        
        
    """ Step 04: Generate ESA file """
    if 4 in steps_to_run:
        print('Step 04: Generate ESA files')
        geomToESA(geometry_enlarged, name_area, db_params, directories_dict['working_directory'], directories_dict['database_directory'], tables_dict['table_esa'], tables_dict['table_esa2grid'], schemas_dict['esa'], schemas_dict['esa'], 'a')
    
    """ Step 05: Generate Distances """
    if 5 in steps_to_run:
        print('Step 04: Generate distances')
        areaToDistances(name_area, 'foot', directories_dict['osm_directory'], directories_dict['osrm_directory'],  schemas_dict['distances'], db_params)

    """ Step 06: Compute indices and generate raster with new indices """
    if 6 in steps_to_run:
        print('Step 06: Compute indices and generate raster with new indices')
        areaToIndex(name_area, index_params['name'], index_params, db_params, directories_dict['working_directory'],  tables_dict['table_index'], directories_dict['database_directory'])

    """ Step 07: Keep additional 'new' files in DB or drop them """
    if 7 in steps_to_run:
        if keep==False:
            print('Step 07: Delete file not needed')
            cleanDB(name_area, directories_dict, tables_dict, schemas_dict, db_params,False)   
    
    print('All done. The new area has been added to the database')



def geomToCitiesBoundary(geometry:shapely.geometry, name_area:str, table_name:str, schema:str, db_params:dict):

    """
    Upload the geometry of the new area to the database
    
    -------------------------------------------------------   
    
    Parameters:
    
    geometry: geometry of the new area for the study. It should be provided in EPSG:4326 / WGS84.
    name_area: name of the new area to be used in the database
    table_name: name of the table on the database
    schema: schema of the table on the database
    db_params: database account
        
    -------------------------------------------------------   
    
    Return:
    empty
    
    """

    df=pd.DataFrame({'city':[name_area], 'geom':[geometry]})
    gdf2psql(df, f"{table_name}", db_params, if_exists='append', index=False, index_label=['city'], schema=f"{schema}")
    
        
def geomToPop(geometry:shapely.geometry, name_area:str, working_directory:str, db_directory:str, table_name:str, db_params:dict, file:str):
    
    """
    Get population raster for the selected area and upload on the database
    
    -------------------------------------------------------   
    
    Parameters:
    
    geometry: geometry of the new area for the study. It should be provided in EPSG:4326 / WGS84.
    name_area: name of the new area to be used in the database
    working_directory: working directory for temporary saving.
    db_directory: database directory for reading
    table_name: name of the table where the raster has to be uploaded
    db_params: database account
    file: reading tiff file 
        
    -------------------------------------------------------   
    
    Return:
    empty
    
    """
    #Get Clipped raster
    xds=getClippedRaster("population", file, geometry, True, True, f"{working_directory}", name_area)

    rast2sql(f"{working_directory}", name_area, f"{db_directory}", f"{table_name}", "a", (xds.shape[2],xds.shape[1]), 'tiff', '4326')
    sqlRasterTable2db(f"{table_name}",f"{db_directory}",db_params)

    #Remove local file
    os.remove(f"{working_directory}/{name_area}.tiff")
    os.remove(f"{db_directory}/{table_name}.sql")
    
       
def geomToOSM(geometry:shapely.geometry, continent:str, db_params:dict, working_directory:str, osm_directory:str, dictionaries_directory:str, name_area:str, schema:str, validation:bool):
    
    """
    From geometry get all OSM files required to generate the index
    
    -------------------------------------------------------   
    
    Parameters:
    
    geometry: geometry of the new area for the study. It should be provided in EPSG:4326 / WGS84.
    continent: continent of the new area, according to OSM continent split from geofabrik: https://download.geofabrik.de/. 
    db_params: database account
    working_directory: working directory for temporary saving.
    dictionaries_directory: directory with dictionaries
    name_area: name of the new area to be used in the database
    validation: boolean for whether to generate files for validation of the green areas or not
    remapping: boolean for whether to generate files with remapping
    
    -------------------------------------------------------   
    
    Return:
    empty
    
    """
    
    # STEP 1: Get OSM extract from continent-wide osm.pbf
    print('Starting extraction of osm.pbf file')
    geomToOSMextract(geometry, continent, working_directory, osm_directory, name_area) 
    print('Completed')
    
    # STEP 2: 
    #Features related
    print('Starting features extraction')
    featuresToextract_greenblue=pickle.load(open(f'{dictionaries_directory}/featuresToextract_greenblue.pickle', 'rb'))
    featuresTodrop_greenblue=pickle.load(open(f'{dictionaries_directory}/featuresTodrop_greenblue.pickle', 'rb'))

    # Extract geometry 
    areaToOSMfeatures(name_area, osm_directory, featuresToextract_greenblue, featuresTodrop_greenblue, name_area, schema, working_directory, dictionaries_directory, db_params)
    
    if validation==True:
        featuresToextract_confounders=pickle.load(open(f'{dictionaries_directory}/featuresToextract_confounders.pickle', 'rb'))
        featuresTodrop_confounders={}
        areaToOSMfeatures(name_area, osm_directory, featuresToextract_confounders, featuresTodrop_confounders, name_area, schema,  working_directory, dictionaries_directory, db_params)
    
    #Generate indices
    conn = psycopg2.connect(
           dbname=db_params['db_name'], user=db_params['db_user'], password=db_params['db_password'], host=db_params['db_host'])
    conn.autocommit=True
    sql=f"""CREATE INDEX "idx_osm_{name_area}" ON {schema}."{name_area}" USING gist (geom)"""
    conn.cursor().execute(sql)
    conn.close()
    print('Completed')

    # Remap geometry
    print('Starting remapping of features')
    areaRemappingOSM(name_area, db_params, "green", [])
    if validation==True: 
        confounders=['grave_yard','cemetery','school','kindergarten','education',
             'allotments', 'orchard', 'flowerbed','greenfield','farmland',
             'vineyard', 'farm','farmyard','construction',
             'golf_course','sports_centre','pitch',
             'horse_riding','scrub','moor', 'heath', 
             'tree_row', 'fell', 'wetland','aerodrome']
        areaRemappingOSM(name_area, db_params, "confounders", confounders)
    print('Completed')
                    
def geomToOSMextract(geometry:shapely.geometry, continent:str, working_directory:str, to_directory:str, name_area:str):
    
    """
    Generate OSM extract for the selected geometry
    
    -------------------------------------------------------   
    
    Parameters:
    
    geometry: geometry of the new area for the study. It should be provided in EPSG:4326 / WGS84.
    continent: continent of the new area, according to OSM continent split from geofabrik: https://download.geofabrik.de/. 
    db_params: database parameters to establish connection
    working_directory: working directory for temporary saving.
    to_directory: directory to save osm.pbf file
    name_area: name of the new area to be used in the database
        
    -------------------------------------------------------   
    
    Return:
    empty
    
    """
    
    geometry.to_file(f"{working_directory}/{name_area}.geojson", driver='GeoJSON')

    #Generate OSM extract for areas of interest using osmium
    osm_file_name=f"/mnt/work/accessToGreen/data/raw_data/osm/planet/{continent}-latest.osm.pbf"

    output_file_name=f"{to_directory}/{name_area}.osm.pbf"
    boundary=f"{working_directory}/{name_area}.geojson"

    #Use smart strategy to ensure relations are complete
    os.system("osmium extract -p '%s' --output='%s' --overwrite '%s' --strategy=smart" %(boundary,output_file_name, osm_file_name))
    os.remove(boundary)

    
def areaToOSMfeatures(name_area:str, from_directory:str, featuresToextract:dict, featuresTodrop:dict, table:str, schema:str, working_directory:str, dictionaries_directory:str, db_params:dict):
    
    """
    Generate OSM extract for the selected geometry
    
    -------------------------------------------------------   
    
    Parameters:
    
    name_area: name of the new area to be used in the database
    from_directory: directory for reading of osm.pbf file
    featuresToextract: dictionary of features to extract
    featuresTodrop: dictionary of features to drop from extraction
    table: name of the table on the database where data are uploaded
    schema: name of the schema on the database where the table is hosted
    working_directory: directory for temporary saving
    dictionaries_directory: directory with dictionaries
            
    -------------------------------------------------------   
    
    Return:
    empty
    
    """
    
    osm_mask_values=pickle.load(open(f'{dictionaries_directory}/osm_mask_values.pickle', 'rb'))
    osm_mask_keys=pickle.load(open(f'{dictionaries_directory}/osm_mask_keys.pickle', 'rb'))
    osm_mask_elements=pickle.load(open(f'{dictionaries_directory}/osm_mask_elements.pickle', 'rb'))
    osm_mask_categories=pickle.load(open(f'{dictionaries_directory}/osm_mask_categories.pickle', 'rb'))
    osm_macroclasses=pickle.load(open(f'{dictionaries_directory}/osm_macroclasses.pickle', 'rb'))

    gdfs_list=[]
    gdfs_list.append(waysExtraction(f"{from_directory}/{name_area}.osm.pbf", featuresToextract, True, True))
    gdfs_list.append(relationsExtraction(f"{from_directory}/{name_area}.osm.pbf", featuresToextract, True, True))
    gdf=pd.concat(gdfs_list)

    if len(featuresTodrop.keys())!=0:
        gdfs_list=[]
        gdfs_list.append(waysExtraction(f"{from_directory}/{name_area}.osm.pbf", featuresTodrop, False, True))
        gdfs_list.append(relationsExtraction(f"{from_directory}/{name_area}.osm.pbf", featuresTodrop, False, True))
        gdf_to_drop=pd.concat(gdfs_list)

    if len(gdf)!=0:
        if len(featuresTodrop.keys())!=0 and len(gdf_to_drop)!=0:
            gdf=gpd.overlay(gdf,gdf_to_drop, how='difference', keep_geom_type=True, make_valid=True)

        #Apply masks for categories
        gdf['category']=gdf['osm_value'].apply(lambda x: osm_mask_categories[osm_macroclasses[x]])
        gdf['osm_value']=gdf['osm_value'].apply(lambda x: osm_mask_values[x])
        gdf['osm_key']=gdf['osm_key'].apply(lambda x: osm_mask_keys[x])
        gdf['osm_element']=gdf['osm_element'].apply(lambda x: osm_mask_elements[x])
        gdf['city']=name_area

        boundary=query4cityboundary(name_area, db_params, 20000)

        gdf=gpd.overlay(gdf, boundary, how='intersection', keep_geom_type=True, make_valid=True)
        gdf.to_file(f"{working_directory}/{name_area}.geojson", driver="GeoJSON")

        #Save into database
        geojson2db(f'{schema}."{name_area}"',f"{working_directory}", name_area, db_params, '')
        #Drop local file to save storage    
        os.remove(f"{working_directory}/{name_area}.geojson")

def areaRemappingOSM(name_area:str, db_params:dict, which:str, confounders_list:list):
    
    """
    Generate OSM extract for the selected geometry
    
    -------------------------------------------------------   
    
    Parameters:
    
    name_area: name of the new area to be used in the database
    db_params: database parameters to establish connection
    which: whether the remapping is for selected green areas or for confounders
    confounders_list
            
    -------------------------------------------------------   
    
    Return:
    empty
    
    """
    
    if which=="green":
        osm_greencombinations=pickle.load(open('/mnt/work/accessToGreen/data/dicts/osm_greencombinations.pickle', 'rb'))
        dfs_list=[]
        for key,value in osm_greencombinations.items():
            df=query4osm2grid(name_area, 'category', value, db_params, 0, 0)
            # Round values
            for VAR in ['green_size', 'size_intersection']:
                df[VAR]=np.round(df[VAR], 2)
            #Rename variables
            df.rename(columns={"green_size":f"{key}_gs", "size_intersection":f"{key}_si"}, inplace=True)
            df['xy_count']=df.groupby(['x', 'y']).cumcount()
            #Generate fake additional variable for merge - it is used to ensure that elements are not duplicated during the merge
            dfs_list.append(df)

        df=dfs_list[0].copy()
        for i in dfs_list[1:]:
            df=pd.merge(df, i, on=['x','y', 'xy_count'], how='outer')
        df=df.fillna(0)
        df.drop(columns=['xy_count'], inplace=True)
        df['city']=name_area
        df2psql(df, "osm2grid", db_params, if_exists='append', index=False, index_label=['x','y', 'city'], schema='osm')
        
    elif which=="confounders":

        df=query4osm2grid(name_area, 'osm_value', confounders_list, db_params, 0, 0)
        # Round values
        for VAR in ['green_size', 'size_intersection']:
            df[VAR]=np.round(df[VAR], 2)
        #Rename variables
        df.rename(columns={"green_size":f"gs", "size_intersection":f"si"}, inplace=True)

        df=df.fillna(0)
        print(name_area)
        df['city']=name_area

        df2psql(df, "osmconfounders2grid", db_params, if_exists='append', index=False, index_label=['x','y', 'city'], schema='osm')
    
    else:
        raise Exception("Error: 'which' ammissible values: ['green', 'confounders']")
           
def geomToESA(geometry, name_area, db_params, working_directory, database_directory, table_esa, table_esa2grid, schema_esa, schema_esa2grid, mode):
    
    """
    From geometry get all OSM files required to generate the index
    
    -------------------------------------------------------   
    
    Parameters:
    
    geometry: geometry of the new area for the study. It should be provided in EPSG:4326 / WGS84.
    name_area: name of the new area to be used in the database
    db_params: database parameters to establish connection
    working_directory: working directory for temporary saving.
    database_directory: database directory
    table: name of the table where the raster has to be uploaded 
    schema: name of the schema where the table is hosted
    mode: whether the table has to be created or the raster has to be appended to an existing table
    
    -------------------------------------------------------   
    
    Return:
    empty
    
    """
    print('Starting extraction of esa features to population grid')
    geomToESAraster(geometry, name_area, db_params, working_directory, database_directory, table_esa, mode, schema_esa)
    print('Completed')
    
    print('Starting remapping of esa features to population grid')
    areaRemappingESA(name_area, db_params, table_esa2grid, ['10', '20', '30'], schema_esa2grid)
    print('Completed')
    
def geomToESAraster(geometry:shapely.geometry, name_area:str, db_params:dict, working_directory:str, db_reading_directory:str, table:str, mode:str, schema:str):    
    
    """
    Get ESA raster and upload it on the database
    
    -------------------------------------------------------   
    
    Parameters:
    
    geometry: geometry of the new area for the study. It should be provided in EPSG:4326 / WGS84.
    name_area: name of the new area to be used in the database
    db_params: database parameters to establish connection
    working_directory: working directory for temporary saving.
    db_reading_directory: database directory
    table: name of the table where the raster has to be uploaded 
    mode: whether the table has to be created or the raster has to be appended to an existing table
    schema: name of the schema where the table is hosted
    
    -------------------------------------------------------   
    
    Return:
    empty
    
    """
    
    wcesa2raster(geometry, working_directory, True, name_area)
    files_to_load=[i.replace(".tiff", "") for i in os.listdir(working_directory) if f"{name_area}_ntile" in i]
    for f in files_to_load:
        rast2sql(f"{working_directory}", f, db_reading_directory, table,  mode, (250,250), 'tiff', '4326')
        sqlRasterTable2db(table, db_reading_directory,db_params, schema)
        

def areaRemappingESA(name_area, db_params, table, codes, schema): 
    
    """
    Pipeline to remap ESA to popultion grid
    
    -------------------------------------------------------   
    
    Parameters:
    
    name_area: name of the new area to be used in the database
    db_params: database parameters to establish connection
    table: name of the table where the raster has to be uploaded 
    codes: ESA codes to be remapped
    schema: name of the schema where the table is hosted
    
    -------------------------------------------------------   
    
    Return:
    empty
    
    """
    
    df=query4esa2grid(name_area, codes, db_params, 0, 0)
    # Round values
    for VAR in ['green_size', 'size_intersection']:
        df[VAR]=np.round(df[VAR], 2)
    #Rename variables
    df.rename(columns={"green_size":f"{0}_gs", "size_intersection":f"{0}_si"}, inplace=True)
    df['city']=name_area
    df2psql(df, table, db_params, if_exists='append', index=False, index_label=['x','y', 'city'], schema=schema)
    
    
    
    
def areaToDistances(name_area:str, profile:str, osm_directory:str, osrm_directory:str,  schema_distances:str, db_params:dict):
    
    """
    Pipeline to compute distances 
    
    -------------------------------------------------------   
    
    Parameters:
    
    name_area: name of the new area to be used in the database
    profile:
    working_directory:
    osrm_directory:
    db_params: database parameters to establish connection
        
    -------------------------------------------------------   
    
    Return:
    empty
    
    """
    
    
    #Identify set of pairs
    df=getDistancesPairs(name_area, db_params) 
    accessible_streets_dict={'highway':['primary','secondary','tertiary','unclassified',
                            'residential','road','living_street','service','track','path','steps','pedestrian','footway']}

    accessible_cells=getNotAccessibleFromHeuristic(name_area, osm_directory, accessible_streets_dict, db_params)
    
    df=pd.merge(df, accessible_cells, left_on=['x_source','y_source'], right_on=['x','y'], how='left')
    df=df.rename(columns={'walk_access':'walk_access_source'})
    df.loc[df['walk_access_source'].isnull()==True, 'walk_access_source']=0
    df=pd.merge(df, accessible_cells, left_on=['x_dest','y_dest'], right_on=['x','y'], how='left')
    df=df.rename(columns={'walk_access':'walk_access_dest'})
    df.loc[df['walk_access_dest'].isnull()==True, 'walk_access_dest']=0
    df['walk_access']=df[['walk_access_source', 'walk_access_dest']].min(axis=1)
    df=df[['lat_source', 'long_source','x_source', 'y_source','walk_access_source','lat_dest', 'long_dest', 'x_dest', 'y_dest','walk_access_dest','walk_access']]
    
    #Compute actual distances
    df=getDistances(df , name_area, profile, osrm_directory, osm_directory) 
    
    #Load distances
    loadDistances(df, name_area, name_area, schema_distances, db_params, True)



    
def getDistancesPairs(name_area:str, db_params:dict):
    
    """
    Get pairs of cells for the computation of distances
    
    -------------------------------------------------------   
    
    Parameters:
    
    name_area: name of the new area to be used in the database
    db_params: database parameters to establish connection
        
    -------------------------------------------------------   
    
    Return:
    pandas.dataframe
    
    """
    
    
    grid=query4grid(name_area, db_params)
    grid['lat']=grid.geom.centroid.y
    grid['long']=grid.geom.centroid.x

    #As origin cells only keep cells within original city boundary
    grid_source = grid.query("inbound == 1")
    #Reproject it to ensure identification of buffered area within which to compute distances
    grid_source=project_gdf(grid_source)
    grid_dest = grid.copy()

    #Identify for each cell, buffered area of 3 km [geodesic], within which to compute distances
    grid_source['geom'] = grid_source.geometry.centroid.buffer(3000)
    #Reproject to standard CRS 
    grid_source = grid_source.to_crs('epsg:4326')
    grid_dest = grid_dest.to_crs('epsg:4326')
    grid = grid.to_crs('epsg:4326')

    for var in ['lat','long', 'x', 'y']:
        grid_source.rename(columns={var:f"{var}_source"}, inplace=True)
        grid_dest.rename(columns={var:f"{var}_dest"}, inplace=True)
    df = grid_source[['lat_source','long_source','geom', 'x_source', 'y_source']].sjoin(grid_dest[['lat_dest','long_dest','geom', 'x_dest', 'y_dest']])
    return df


    
def getNotAccessibleFromHeuristic(name_area:str, osm_directory:str, accessible_streets_dict:dict, db_params:dict):
    
    """
    Identify cells with no streets in OSM and as such to be excluded from the computation
    
    -------------------------------------------------------   
    
    Parameters:
    
    name_area: name of the new area to be used in the database
    osm_directory: location of osm.pbf file
                    
    -------------------------------------------------------   
    
    Return:
    pandas.dataframe
    
    """
    
    file=f"{osm_directory}/{name_area}.osm.pbf"
    
    streets=waysExtraction(file, accessible_streets_dict, True, False)

    #Both datasets are in crs EPSG:4326
    grid=query4grid(name_area, db_params)
    grid['lat']=grid.geom.centroid.y
    grid['long']=grid.geom.centroid.x
    accessible=gpd.sjoin(grid, streets, how='left', predicate='intersects')
    accessible_cells=accessible[accessible['osm_key'].isnull()==False][['x', 'y']].drop_duplicates()
    accessible_cells=accessible_cells[['x','y']]
    accessible_cells['walk_access']=1
    
    return accessible_cells
            
      
def getDistances(df:pd.DataFrame, name_area:str, profile:str, osrm_working_folder:str, osm_inputfolder:str):
    
    """
    Compute distances for selected cells pairs
    
    -------------------------------------------------------   
    
    Parameters:
    
    df, name_area:str, profile:str, working_folder, osrm_working_folder, osm_inputfolder, output_directory, return_df:bool=True, save_df:bool=False
    
    name_area: name of the new area to be used in the database
    directories_dict: dictionary with list of directories. It must include two keys: 
                    'osm_directory': location of osm.pbf file
                    'working_directory': only required if the dataframe is saved in a local csv file, it is the location where the csv file is published.
    return_df: boolean. True if the dataframe has to be returned, False otherwise. [Default value: True]
    save_df: boolean. True if the dataframe has to be returned, False otherwise. [Default value: True]
    filename: only required if the dataframe is saved in a local csv file, it is the name of the file
    
        
    -------------------------------------------------------   
    
    Return:
    pandas.dataframe 
    
    """

    if len(df[df['walk_access']==1])>0:
        print('start')
        res=osrm_files_creation(name_area, osm_inputfolder, osrm_working_folder, profile)
        if res.returncode!=0:
            raise Exception('Error: osrm-related files not correctly created.')
        else:
            all_done=0
            step=0
            df['walk_durations']=np.nan
            while all_done==0:
                print(str(name_area)+" "+str(step)+", missing pairs:"+str(len(df)-len(df[df['walk_durations'].isnull()==False])))
                subset=coords_vector_identification(df, 2000, f"{name_area}_coords.csv", osrm_working_folder)    #vector size limit of 2000 chosen to optimize computational time
                res=one_run_osrm(name_area, f"{name_area}_coords.csv", f"{name_area}_durs.csv", osrm_working_folder)
                if res.returncode!=0:
                    raise Exception('Error: osrm run not correctly completed.')
                else:
                    df=merge_one_run(df, subset,f"{name_area}_durs.csv", osrm_working_folder)
                    step=step+1
                    if len(df[(df['walk_durations'].isnull()==True) & (df['walk_access']==1)])==0:
                        all_done=1
                    os.chdir(osrm_working_folder)
            #Transform in minutes
            #Uncomputed distances are stored as string - coerce
            df['walk_durations']=pd.to_numeric(df['walk_durations'], errors='coerce')
            df['walk_minutes']=df['walk_durations']/60
            osrm_files_deletion(name_area, f"{name_area}_coords.csv", f"{name_area}_durs.csv" , osrm_working_folder)
            
            return df[['x_source', 'y_source', 'lat_source', 'long_source','walk_access_source', 'x_dest', 'y_dest','lat_dest', 'long_dest', 'walk_access_dest', 'walk_access', 'walk_minutes']]
    else:
        raise Exception('Error: no accessible cells in the selected area.')

def loadDistances(df:pd.DataFrame, name_area:str,table:str, schema:str, db_params:dict, compute_geodesic:bool=True):
    
    """
    Load distances in DB
    
    -------------------------------------------------------   
    
    Parameters:
    
    df:
    name_area:
    table:
    schema:
    db_params:
    compute_geodesic:
        
    -------------------------------------------------------   
    
    Return:
    empty
    
    """
                                                                   
    df['city']=name_area
    
    ### Generate geodesic distances
    if compute_geodesic==True:
        df['geodesic_meters']=df.apply(lambda x: geodesic((x['lat_source'], x['long_source']),(x['lat_dest'], x['long_dest'])).km*1000, axis=1)
        #Transform in minutes assuming 5km/h converting factor
        df['geodesic_minutes']=df['geodesic_meters']*(60)/5000
    #keep only relevant columns
    ind_cols=['city', 'x_source', 'y_source', 'x_dest', 'y_dest']
    if compute_geodesic==True:
        dist_cols=['walk_minutes', 'geodesic_meters','geodesic_minutes']
    else:
        dist_cols=['walk_minutes']
                                    
    df=df[[*ind_cols,*dist_cols]]

    #Round to integer
    for VAR in dist_cols:
        df[VAR]=np.round(df[VAR], 1)
    
    df2psql(df, table, db_params, if_exists='append', index=False, index_label=[ 'city', 'x_source','y_source', 'x_dest','y_dest'], schema=schema)                            
                            
        
                            
def areaToIndex(name_area:str, index_name:str, index_params:dict, db_params:dict, working_folder:str,table:str, db_directory:str):
    
    """
    Produce index and load id using two ad hoc function
    
    -------------------------------------------------------   
    
    Parameters:
    
    df:
    name_area:
    table:
    schema:
    db_params:
    compute_geodesic:
        
    -------------------------------------------------------   
    
    Return:
    empty
    
    """
                                     
    min_intersection=min(0.025,index_params['min_park_size'])
    index=accessibility_index_pipeline(name_area, index_params, index_name, db_params, min_intersection)
    indexToRaster(index, name_area, db_params, working_folder, table, db_directory)
        
        
                                    
def indexToRaster(df:pd.DataFrame, name_area:str, db_params:dict, working_folder:str, table:str, db_directory:str):
    
    """
    Load index as a raster on DB
    
    -------------------------------------------------------   
    
    Parameters:
    
    df:
    name_area:
    db_params:
    table:
    schema:
    db_params:
    compute_geodesic:
        
    -------------------------------------------------------   
    
    Return:
    empty
    
    """
   
                       
    #Generate raster
    grid=query4grid_unmasked(name_area, db_params)
    grid.loc[grid[['x','y']].apply(tuple, axis=1).isin(list(df[['x','y']].apply(tuple, axis=1)))==False, 'population']=0
    grid=pd.merge(grid, df, how='left', on=['x', 'y'])
    grid.fillna(-2)

    grid['geom']=grid['geom'].centroid

    points = list(zip(np.round(grid.geometry.x,5),np.round(grid.geometry.y,5)))
    xRange=np.round(grid.geometry.x,5).unique()
    yRange=np.round(grid.geometry.y,5).unique()

    #define raster resolution
    xRes = (np.round(grid.geometry.x,5).max()-np.round(grid.geometry.x,5).min())/(len(xRange)-1)
    yRes = (np.round(grid.geometry.y,5).max()-np.round(grid.geometry.y,5).min())/(len(yRange)-1)

    #definition of the raster transform array
    from rasterio.transform import Affine
    transform = Affine.translation(xRange[0]-xRes/2, yRange[-1]-yRes/2)*Affine.scale(xRes,yRes)

    #get crs as wkt
    from rasterio.crs import CRS
    rasterCrs = CRS.from_epsg(4326)
    rasterCrs.data

    #definition, register and close of interpolated raster
    cols=[*['population'],*list(set(df.columns)-set(('x', 'y')))]
    interpRaster = rasterio.open(f"{working_folder}/{name_area}.tiff",
                                    'w',
                                    driver='GTiff',
                                    height=len(yRange),
                                    width=len(xRange),
                                    count=len(cols),
                                    dtype=rasterio.dtypes.float64,
                                    crs=rasterCrs,
                                    transform=transform,
                                    nodata=-2)
    
    for ind_col,col in enumerate(cols):
        grid[col]=np.round(grid[col].values,2)
        mat = np.array(list(grid.sort_values(by=['x','y'], ascending=True)[col]))
        mat = np.squeeze(mat)  
        mat = mat.reshape(len(xRange),len(yRange)).T #Because reshape orders by row instead of columns
        mat=np.flip(mat,0)
        interpRaster.write(mat,ind_col+1)

    interpRaster.close()

    rast2sql(working_folder, name_area , db_directory, table, "a", (len(yRange),len(xRange)), 'tiff', '4326')
    sqlRasterTable2db(table, db_directory, db_params)

    #Remove local file
    os.remove(f"{working_folder}/{name_area}.tiff")
    os.remove(f"{db_directory}/{table}.sql")


def cleanDB(name_area:str, directories_dict:dict, tables_dict:dict, schemas_dict:dict, db_params:dict, remove_index:bool=True):
    
    """
    Clean from temporary files
    
    -------------------------------------------------------   
    
    Parameters:
    
    name_area
    directories_dict
    tables_dict
    schemas_dict
        
    -------------------------------------------------------   
    
    Return:
    empty
    
    """
    
    ### Remove local files
    # osm file
    cmd=f"rm {directories_dict['osm_directory']}/{name_area}.osm.pbf"
    subprocess.run(cmd,shell=True)

    # esa tiles
    cmd=f"rm {directories_dict['working_directory']}/{name_area}_ntile*"
    subprocess.run(cmd,shell=True)
 
    # distances 
    cmd=f"rm {directories_dict['distances_directory']}/{name_area}.dist.bz2"
    subprocess.run(cmd,shell=True)

                                       
    #Remove related files in database
    conn = psycopg2.connect(
    dbname=db_params['db_name'], user=db_params['db_user'], password=db_params['db_password'], host=db_params['db_host'])
    conn.autocommit=True
    with conn.cursor() as cur: 
        #cities_boundary
        cur.execute(f"""DELETE FROM {schemas_dict['cities_boundary']}.{tables_dict['table_cities_boundary']} WHERE city='{name_area}'""")
        #population
        cur.execute(f"""DELETE FROM {schemas_dict['population']}.{tables_dict['table_population']} WHERE filename='{name_area}.tiff'""")
        #osm
        cur.execute(f"""DROP TABLE {schemas_dict['osm']}."{name_area}" """)
        cur.execute(f"""DELETE FROM {schemas_dict['osm']}.{tables_dict['table_osm2grid']} WHERE city='{name_area}' """)
        cur.execute(f"""DELETE FROM {schemas_dict['osm']}.{tables_dict['table_osmconfounders2grid']} WHERE city='{name_area}' """)
        #esa
        cur.execute(f"""DELETE FROM {schemas_dict['esa']}.{tables_dict['table_esa']} WHERE filename LIKE '{name_area}_ntile_%%'""")
        cur.execute(f"""DELETE FROM {schemas_dict['esa']}.{tables_dict['table_esa2grid']} WHERE city='{name_area}'""")
        #distances
        cur.execute(f"""DROP TABLE {schemas_dict['distances']}."{name_area}" """)
        #index
        if remove_index==True:
            cur.execute(f"""DELETE FROM {schemas_dict['index']}.{tables_dict['table_index']} WHERE filename='{name_area}.tiff'""")
    conn.close()
    
        
