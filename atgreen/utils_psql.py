from .basic import *
import psycopg2 
import subprocess
from sqlalchemy import create_engine, Float
from geoalchemy2 import Geometry, WKTElement

def getListOfAreas(db_params:dict):
    
    """
    
    Get list of areas in the database
    -------------------------------------------------------  
    
    Parameters:
    
    db_params= dictionary with info to access db
    **kwargs= optional argument from pandas.dataframe.to_sql()
    
    -------------------------------------------------------  
    Return:
    empty
    
    """
    
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")
    
    sql="""SELECT DISTINCT city 
           FROM public.cities_boundary
        """
    to_return=list(pd.read_sql_query(sql,engine)['city'])
    engine.dispose()
    
    return to_return



def dict2psql(dict_to_pass:dict, tablename:str, key_name:str, value_name:str, db_params:dict, **kwargs):
    
    """
    Load dictionary as table in sql
    -------------------------------------------------------  
    
    Parameters:
    
    dict_to_pass=dictionary to load
    tablename=name of the table
    key_name=column name to assign to dictionary keys
    value_name=column name to assign to dictionary values
    db_params= dictionary with info to access db
    **kwargs= optional argument from pandas.dataframe.to_sql()
    
    -------------------------------------------------------  
    
    Return:
    empty
    """
    
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")
    df=pd.DataFrame({key_name: list(dict_to_pass.keys()), value_name: list(dict_to_pass.values())})
    df.to_sql(tablename, con=engine,**kwargs)
    engine.dispose()
    
def df2psql(df:pd.DataFrame, tablename:str, db_params:dict, **kwargs):
    
    """
    Load dataframe as table in sql
    -------------------------------------------------------  
    
    Parameters:
    
    df=pandas dataframe
    tablename=name of the table
    db_params= dictionary with info to access db
    **kwargs= optional argument from pandas.dataframe.to_sql()
    
    -------------------------------------------------------  
    
    Return:
    empty
    """
    
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")
    df.to_sql(tablename, con=engine,**kwargs)
    engine.dispose()
    
def gdf2psql(gdf:gpd.GeoDataFrame, tablename:str, db_params:dict, **kwargs):
    
    """
    Load dataframe as table in sql
    -------------------------------------------------------  
    
    Parameters:
    
    gdf=geopandas dataframe
    tablename=name of the table
    db_params= dictionary with info to access db
    **kwargs= optional argument from pandas.dataframe.to_sql()
    
    -------------------------------------------------------  
    
    Return:
    empty
    """
    
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")


    # Use 'dtype' to specify column's type
    # For the geom column, we will use GeoAlchemy's type 'Geometry'
    if 'geom' in list(gdf.columns):
        gdf['geom'] = gdf['geom'].apply(lambda geom: WKTElement(geom.wkt, srid=4326))
        gdf.to_sql(tablename, con=engine,dtype={'geom': Geometry('Multipolygon', srid=4326)} ,**kwargs)
    else:
        gdf['geometry'] = gdf['geometry'].apply(lambda geom: WKTElement(geom.wkt, srid=4326))
        gdf.to_sql(tablename, con=engine,dtype={'geometry': Geometry('Multipolygon', srid=4326)} ,**kwargs)
    engine.dispose()


def rast2sql(input_dir:str, filename:str, output_dir:str, tablename:str, mode:str="c", raster_dim:tuple=(256,256), raster_format:str='tiff', crs:str='4326'):
    """
    Run the raster loader executable raster2pgsql that loads GDAL supported raster formats into sql suitable for loading into a PostGIS raster table
    -------------------------------------------------------  
    
    Parameters:
    
    input_dir: input folder with list of raster
    output_dir: directory for saving of .sql file
    tablename: name of the table 
    raster_format: raster format [DEFAULT: 'tiff']
    crs: Coordinate reference system of the raster [DEFAULT: EPSG:4326]  
    
    ------------------------------------------------------- 
    
    Return
    empty
    
    """  
    cmd=f'raster2pgsql -{mode} -s {crs} -I -M {input_dir}/{filename}.{raster_format} -F -t {str(raster_dim[0])}x{str(raster_dim[1])} public.{tablename} > {output_dir}/{tablename}.sql'
    res=subprocess.run(cmd,shell=True)
    if res.returncode!=0:
        raise Exception("The requested command was unsuccessfull. Please check input arguments.")
                
        
def multiplerast2sql(input_dir:str, output_dir:str, tablename:str, raster_format:str='tiff', crs:str='4326'):
    """
    Run the raster loader executable raster2pgsql that loads GDAL supported raster formats into sql suitable for loading into a PostGIS raster table - for multiple rasters at once
    
    ------------------------------------------------------- 
    
    Parameters: 
    
    input_dir: input folder with list of raster
    output_dir: directory for saving of .sql file
    tablename: name of the table 
    raster_format: raster format [DEFAULT: 'tiff']
    crs: Coordinate reference system of the raster [DEFAULT: EPSG:4326]  
    
    ------------------------------------------------------- 
    
    Return:
    empty
    """  
    cmd=f'raster2pgsql -d -s {crs} -I -M {input_dir}/*.{raster_format} -F -t "auto" public.{tablename} > {output_dir}/{tablename}.sql'
    res=subprocess.run(cmd,shell=True)
    if res.returncode!=0:
        raise Exception("The requested command was unsuccessfull. Please check input arguments.")


def sqlRasterTable2db(tablename:str, directory:str, db_params: dict, schema:str='public'):
    
    """
    Load SQL raster table into database
    ------------------------------------------------------- 

    Parameters: 
    
    tablename: name of the SQL raster table to load (as obtained from rast2db)
    directory: directory of the raster table to load
    db_params: dictionary of database parameters    
    
    ------------------------------------------------------- 
    Return:
    empty

    """ 
    
    if schema=="public":
        cmd="PGPASSWORD=%s psql -h localhost --user=%s  --dbname=%s --file=%s.sql" %( db_params['db_password'], db_params['db_user'], db_params['db_name'], directory+"/"+tablename)
        print(cmd)
        res=subprocess.run(cmd, shell=True)
        if res.returncode!=0:
            raise Exception("The requested command was unsuccessfull. Please check input arguments.")
    else:
        ### Generate tmp table in public
        conn = psycopg2.connect(
        dbname=db_params['db_name'], user=db_params['db_user'], password=db_params['db_password'], host=db_params['db_host'])
        conn.autocommit=True
        with conn.cursor() as cur: 
            cur.execute(f"""CREATE TABLE public."{tablename}" AS SELECT * FROM "{schema}"."{tablename}" LIMIT 0 """)
        conn.close()
        
        cmd="PGPASSWORD=%s psql -h localhost --user=%s  --dbname=%s --file=%s.sql" %( db_params['db_password'], db_params['db_user'], db_params['db_name'], directory+"/"+tablename)
        print(cmd)
        res=subprocess.run(cmd, shell=True)
        if res.returncode!=0:
            raise Exception("The requested command was unsuccessfull. Please check input arguments.")
        
        conn = psycopg2.connect(
               dbname=db_params['db_name'], user=db_params['db_user'], password=db_params['db_password'], host=db_params['db_host'])
        conn.autocommit=True
        with conn.cursor() as cur: 
            cur.execute(f"""
                INSERT INTO "{schema}"."{tablename}"
                SELECT ROW_NUMBER() OVER (ORDER by rid) + (SELECT MAX(rid) FROM "{schema}"."{tablename}") AS rid, rast, filename
                FROM public."{tablename}"
                """)  
            cur.execute(f"""DROP TABLE public."{tablename}" """)
        conn.close()
      
    return res


def geojson2db(tablename:str,  directory:str, filename:str ,db_params: dict, mode:str=''): 
    """
    Load a single geoJSON file into database
    ------------------------------------------------------- 

    Parameters: 
    
    tablename: name of the table on the database
    filename: name of the geoJSON table to load
    directory: directory of the geoJSON table to load
    db_params: dictionary of database parameters 
    mode: overwrite existing layer or append to existing layer
    ------------------------------------------------------- 
    
    Return
    empty
    
    """ 
    
    cmd=f"""ogr2ogr %s -lco LAUNDER=NO -lco GEOMETRY_NAME=geom -f "PostgreSQL" PG:"dbname=%s user=%s password=%s host=%s" %s.geojson -nln "%s" -nlt PROMOTE_TO_MULTI""" %(mode,  db_params['db_name'], db_params['db_user'], db_params['db_password'], db_params['db_host'], directory+"/"+filename, tablename)
    res=subprocess.run(cmd, shell=True)
    if res.returncode!=0:
        raise Exception("The requested command was unsuccessfull. Please check input arguments.")
         
def query4cityboundary(city: str, db_params:dict, buffer:float=0, table_name:str='cities_boundary'):
    
    """ 
    Return city boundary with given buffer in meters
    ------------------------------------------------------- 
    
    Parameters:
    
    city: Name of the city as from DB
    db_params: dictionary with parameters to read from database
    buffer: Buffer in meters
    table_name: name reading table
    
    ------------------------------------------------------- 
    
    Return: 
    gpd.geodataframe with CRS:4326
    """
    
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")
    #Only keep thing that fall within reasonable size from city boundary 
    sql =f"""
        SELECT st_transform(st_buffer(st_setsrid(st_transform({table_name}.geom, _ST_BestSRID({table_name}.geom)), _ST_BestSRID({table_name}.geom)),{buffer}), 4326) as geom
        FROM {table_name}
        WHERE {table_name}.city='{city}'
        """ 
    boundary=gpd.GeoDataFrame.from_postgis(sql,engine, crs='EPSG:4326')
    engine.dispose()
    
    return boundary

        

def query4esa2polygons(city: str, codes: list, db_params: dict):
    """
    Extract polygon of ESA data for selected land cover codes and perform unary_union of all adjacent geometries
    ------------------------------------------------------- 

    Parameters:
    city: city_name
    codes: list. World cover codes to extract. 
    db_params: db parameters to establish connection
    
    ------------------------------------------------------- 
    
    Return:
    geopandas.GeoDataFrame. 
    """
    
    #Query the required data only, filtering on selected value only using Reclass formula
    if len(codes)==1:
        cond="[0-%s):0,%s:1,(%s-100]:0" %(str(codes[0]), str(codes[0]), str(codes[0]))
    elif len(codes)==2:
        codes=sorted(codes)
        cond=f"[0-%s):0,%s:1,(%s-%s):0, %s:1,(%s-100]:0" %(str(codes[0]), str(codes[0]), str(codes[0]), str(codes[1]), str(codes[1]) , str(codes[1]))
    elif len(codes)==3:
        codes=sorted(codes)
        cond=f"[0-%s):0,%s:1,(%s-%s):0, %s:1,(%s-%s):0,%s:1,(%s-100]:0" %(str(codes[0]), str(codes[0]), str(codes[0]), str(codes[1]), str(codes[1]) , str(codes[1]), str(codes[2]), str(codes[2]), str(codes[2]))
    else:
        raise Exception('Up to three codes at once permitted.')

    
    #Establish connection to database     
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")
       
    sql =f"""
        SELECT (ST_DumpAsPolygons(ST_Reclass(rast,1,'{cond}','1BB',0), 1, TRUE)).* 
        FROM esa.esa
        WHERE filename LIKE '{city}_ntile_%%' 
        """ 
    
    gdf=gpd.GeoDataFrame.from_postgis(sql,engine)
    engine.dispose()
    
    return gdf


def query4esa2grid(city: str, codes: list, db_params: dict, min_park_size: float, min_intersection:float):
    """
    The function remap green elements from ESA to population grid for the computation of the accessibility indices.
    Notice that the returned item is not a grid per se, for two reasons:
    1) empty cells are not reported
    2) the same cell may be reported multiple times if several disjoint green polygons intersect with the cell. 
    
    ------------------------------------------------------- 
    
    Parameters:
    city: city_name
    codes: World cover codes to extract
    db_params: db parameters to establish connection
    min_park_size: minimum_size of remapped green polygon
    min_intersection_prop: minimum size of intersected area
    
    ------------------------------------------------------- 
    
    Return:
    pd.DataFrame
    """
    
    #Query the required data only, filtering on selected value only using Reclass formula
    if len(codes)==1:
        cond="[0-%s):0,%s:1,(%s-100]:0" %(str(codes[0]), str(codes[0]), str(codes[0]))
    elif len(codes)==2:
        codes=sorted(codes)
        cond=f"[0-%s):0,%s:1,(%s-%s):0, %s:1,(%s-100]:0" %(str(codes[0]), str(codes[0]), str(codes[0]), str(codes[1]), str(codes[1]) , str(codes[1]))
    elif len(codes)==3:
        codes=sorted(codes)
        cond=f"[0-%s):0,%s:1,(%s-%s):0, %s:1,(%s-%s):0,%s:1,(%s-100]:0" %(str(codes[0]), str(codes[0]), str(codes[0]), str(codes[1]), str(codes[1]) , str(codes[1]), str(codes[2]), str(codes[2]), str(codes[2]))
    else:
        raise Exception('Up to three codes at once permitted.')
           
    #Establish connection to database     
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")

    sql =f"""
    SELECT final.x, final.y, final.green_size/10^4 as green_size , final.size_intersection/10^4 as size_intersection
    FROM 
    (SELECT grid.x, grid.y, green.green_size, ST_AREA(st_setsrid(st_transform(st_intersection(green.geom, grid.geom), _ST_BestSRID(green.geom)), _ST_BestSRID(green.geom))) as size_intersection
    
    FROM 
    
    (SELECT tmp11.geom, ST_AREA(st_setsrid(st_transform(tmp11.geom, _ST_BestSRID(tmp11.geom)), _ST_BestSRID(tmp11.geom))) as green_size
    FROM 
    (SELECT (ST_Dump(tmp1.geom)).geom
    FROM (SELECT ST_Union(esa1.geom) AS geom 
    FROM (SELECT (ST_DumpAsPolygons(ST_Reclass(rast,1,'{cond}','1BB',0), 1, TRUE)).*
        FROM esa.esa
        WHERE filename LIKE '{city}_ntile_%%') AS esa1) AS tmp1) AS tmp11) AS green, 

    (SELECT tmp2.x, tmp2.y, tmp2.geom, ST_AREA(st_setsrid(st_transform(tmp2.geom, _ST_BestSRID(tmp2.geom)), _ST_BestSRID(tmp2.geom))) as cell_size
    FROM (SELECT  (ST_PixelAsPolygons(rast, 1, TRUE)).*
        FROM ghs_pop
        WHERE ghs_pop.filename='{city}.tiff') AS tmp2) AS grid

    WHERE st_intersects(green.geom, grid.geom) = TRUE 
    AND green.green_size/10^4>={min_park_size}) AS final
    WHERE final.size_intersection>={min_intersection}
    """

    df=pd.read_sql_query(sql,engine)
    engine.dispose()
    
    return df


def query4osm2polygons(city: str, osm_feature:str, osm_which:list, db_params: dict):
    """
    Extract polygon of ESA data for selected land cover codes and perform unary_union of all adjacent geometries
    ------------------------------------------------------- 
    
    Parameters:
    city: city_name
    osm_feature: feature column to be used for selection 
    osm_which: which feature to include in the selection
    db_params: db parameters to establish connection
    
    ------------------------------------------------------- 
    
    Return:
    geopandas.GeoDataFrame 
    """
    #Define admitted values 
    valid_features=['category','osm_key', 'osm_value', 'osm_element']
    if osm_feature not in valid_features:
        raise ValueError("results: osm_feature must be one of %r." % valid_features)

        
    osm_classes_table_dict={'category':'osm_mask_categories','osm_key':'osm_mask_keys', 'osm_value':'osm_mask_values', 'osm_element':'osm_mask_elements'}

    #Establish connection to database     
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")
    
    if len(osm_which)==1:
        sql =f"""
            SELECT * 
            FROM osm."{city}"
            WHERE {osm_feature} IN (
                    SELECT value
                    FROM osm.{osm_classes_table_dict[osm_feature]}
                    WHERE osm.{osm_classes_table_dict[osm_feature]}.key = '{osm_which[0]}')
            """ 
    else:
            sql =f"""
            SELECT * 
            FROM osm."{city}"
            WHERE {osm_feature} IN (
                    SELECT value
                    FROM osm.{osm_classes_table_dict[osm_feature]}
                    WHERE osm.{osm_classes_table_dict[osm_feature]}.key IN {tuple(osm_which)})
            """ 
    
    gdf=gpd.GeoDataFrame.from_postgis(sql,engine)
    engine.dispose()
    
    return gdf


def query4osm2grid(city: str, osm_feature:str, osm_which:list, db_params: dict, min_park_size: float, min_intersection:float):
    
    """
    The function remap green elements from ESA to population grid for the computation of the accessibility indices.
    Notice that the returned item is not a grid per se, for two reasons:
    1) empty cells are not reported
    2) the same cell may be reported multiple times if several disjoint green polygons intersect with the cell. 
    ------------------------------------------------------- 
    
    Parameters:
    city: city_name
    osm_feature: features to be used for selection
    osm_which: values of feature to select
    db_params: db parameters to establish connection
    min_park_size: minimum_size of remapped green polygon
    min_intersection_prop: minimum size of intersected area
    
    ------------------------------------------------------- 
    
    Return:
    pd.DataFrame
    """
             
    #Establish connection to database     
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")


    #Define admitted values 
    valid_features=['category','osm_key', 'osm_value', 'osm_element']
    if osm_feature not in valid_features:
        raise ValueError("results: osm_feature must be one of %r." % valid_features)

        
    osm_classes_table_dict={'category':'osm_mask_categories','osm_key':'osm_mask_keys', 'osm_value':'osm_mask_values', 'osm_element':'osm_mask_elements'}

    #Establish connection to database     
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")
    
    if len(osm_which)==1:
        subquery=f"""SELECT value
                    FROM osm.{osm_classes_table_dict[osm_feature]}
                    WHERE osm.{osm_classes_table_dict[osm_feature]}.key = '{osm_which[0]}'"""
    else:
        subquery=f"""SELECT value
                    FROM osm.{osm_classes_table_dict[osm_feature]}
                    WHERE osm.{osm_classes_table_dict[osm_feature]}.key IN {tuple(osm_which)}"""
    
    sql =f"""
    SELECT final.x, final.y, final.green_size/10^4 as green_size , final.size_intersection/10^4 as size_intersection
    FROM 
    (SELECT grid.x, grid.y, green.green_size, ST_AREA(st_setsrid(st_transform(st_intersection(green.geom, grid.geom), _ST_BestSRID(green.geom)), _ST_BestSRID(green.geom))) as size_intersection
    
    FROM 
    
    (SELECT tmp11.geom, ST_AREA(st_setsrid(st_transform(tmp11.geom, _ST_BestSRID(tmp11.geom)), _ST_BestSRID(tmp11.geom))) as green_size
    FROM 
    (SELECT (ST_Dump(tmp1.geom)).geom
    FROM (SELECT ST_Union(osm."{city}".geom) AS geom 
    FROM osm."{city}"
    WHERE {osm_feature} IN (
        {subquery})) AS tmp1) AS tmp11) AS green, 

    (SELECT tmp2.x, tmp2.y, tmp2.geom, ST_AREA(st_setsrid(st_transform(tmp2.geom, _ST_BestSRID(tmp2.geom)), _ST_BestSRID(tmp2.geom))) as cell_size
    FROM (SELECT  (ST_PixelAsPolygons(rast, 1, TRUE)).*
        FROM ghs_pop
        WHERE ghs_pop.filename='{city}.tiff') AS tmp2) AS grid

    WHERE st_intersects(green.geom, grid.geom) = TRUE 
    AND green.green_size/10^4>={min_park_size}) AS final
    WHERE final.size_intersection>={min_intersection}
    """
         
    df=pd.read_sql_query(sql,engine)
    engine.dispose()
    
    return df

def query4grid(city: str, db_params: dict):
    
    """
    Extract polygon of ESA data for selected land cover codes and perform unary_union of all adjacent geometries
    ------------------------------------------------------- 
    
    Parameters:
    city: city_name
    codes: World cover codes to extract
    db_params: db parameters to establish connection
    
    ------------------------------------------------------- 
    Return:
    geopandas.GeoDataFrame. 
    """
    
    #Establish connection to database     
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")
       
    sql =f"""
        CREATE TABLE for_export1 AS 
        SELECT (ST_PixelAsPolygons(rast, 1, TRUE)).* 
        FROM ghs_pop
        WHERE ghs_pop.filename='{city}.tiff';
        
        ALTER TABLE for_export1 ADD COLUMN inbound int;
        UPDATE for_export1 SET inbound=0;
        
        UPDATE for_export1
        SET inbound = 1 
        FROM cities_boundary
        WHERE ST_Intersects(for_export1.geom, cities_boundary.geom)
        AND cities_boundary.city ='{city}';
   
        SELECT * FROM for_export1
        """ 
    
    gdf=gpd.GeoDataFrame.from_postgis(sql,engine).rename(columns={'val':'population'})
    engine.dispose()
    #Set population to 0 if negative or not inbound
    gdf.loc[(gdf['inbound']==0) | (gdf['population']<0), 'population']=0
    
    #Drop table
    conn = psycopg2.connect(
        dbname=db_params['db_name'], user=db_params['db_user'], password=db_params['db_password'], host=db_params['db_host'])
    conn.autocommit=True
    with conn.cursor() as cur: 
        cur.execute(f"""DROP TABLE for_export1""")
    conn.close()

    return gdf

def query4grid_unmasked(city: str, db_params: dict):
    
    """
    Extract polygon of ESA data for selected land cover codes and perform unary_union of all adjacent geometries
    ------------------------------------------------------- 
    
    Parameters:
    
    city: city_name
    db_params: db parameters to establish connection
    
    ------------------------------------------------------- 
    
    Return:
    
    geopandas.GeoDataFrame. 
    """
    
    #Establish connection to database     
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")
       
    sql =f"""
        SELECT (ST_PixelAsPolygons(rast, 1, FALSE)).* 
        FROM ghs_pop
        WHERE ghs_pop.filename='{city}.tiff';
        """ 
    
    gdf=gpd.GeoDataFrame.from_postgis(sql,engine).rename(columns={'val':'population'})
    engine.dispose()
    
    return gdf

def queryRemappedGreen(city:str , tablename:str , col_prefix: int, min_park_size:float, min_intersection:float, db_params: dict):
    
    """
    Extract tables with remapped green information
    ------------------------------------------------------- 
    
    Parameters:
    
    city: city_name
    tablename: name of the table for extraction
    col_prefix: column to extract
    min_park_size: minimum size (in hectares) of the parks to be extrcted
    min_intersection: minimum size (in hectares) of the intersection between cell and park, for the cell to be characterized as green
    db_params: db parameters to establish connection
    
    ------------------------------------------------------- 
    
    Return:
    pandas.DataFrame
    """

    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")
       
    sql =f"""
        SELECT *
        FROM {tablename}
        WHERE city='{city}'
        """  
    df=pd.read_sql(sql, engine)
    engine.dispose()
    
    df=df[df[f"{col_prefix}gs"]>=min_park_size]
    df=df[df[f"{col_prefix}si"]>=min_intersection]
    df['green']=1
    df.rename(columns={f"{col_prefix}gs":"gs",f"{col_prefix}si":"si" }, inplace=True)
    return df[['id','x','y','green', 'gs', 'si']]

def queryDistances(city:str , which_distances:str, db_params: dict):
    """
    Extract distances
    ------------------------------------------------------- 
    
    Parameters:
    
    city: city_name
    which_distances: type of distance to be extracted (geodesic vs street-network)
    db_params: db parameters to establish connection
    
    ------------------------------------------------------- 
    
    Return:
    pandas.DataFrame
    """

    
    dist_dict={'street-network':'walk_minutes', 'geodesic':'geodesic_minutes'}
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")   
    sql =f"""
        SELECT source, dest, {dist_dict[which_distances]} as dist
        FROM distances."{city}"
        """  
    df=pd.read_sql(sql, engine)
    engine.dispose()
    
    return df



def query4raster(city: str, db_params: dict, table:str, schema:str, band:int):
    
    """
    Query raster
    ------------------------------------------------------- 
    
    Parameters:
    city: city_name
    db_params: db parameters to establish connection
    table: table name
    schema: schema where the table is hosted
    band: band of the raster to query
    
    ------------------------------------------------------- 
    Return:
    geopandas.GeoDataFrame. 
    """
    
    #Establish connection to database     
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")
       
    sql =f"""
        SELECT (ST_PixelAsPolygons(rast, {str(band)}, TRUE)).* 
        FROM "{schema}"."{table}"
        WHERE {table}.filename LIKE '{city}.tiff';
        """ 
    
    gdf=gpd.read_postgis(sql,engine).rename(columns={'val':f'band_{str(band)}'})

    return gdf

def query4table(table, schema, db_params, geographic=False):
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")    

    sql=f"""
        SELECT * FROM {schema}."{table}"   
        """
    if geographic==False:
        return pd.read_sql(sql, engine)
    else:
        return gpd.read_postgis(sql,engine)
    
def query4filteredtable(table, schema, db_params, where_col, where_val, geographic=False):
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")    

    sql=f"""
        SELECT * FROM {schema}.{table} WHERE {table}.{where_col}='{where_val}'
        """
    if geographic==False:
        return pd.read_sql(sql, engine)
    else:
        return gpd.read_postgis(sql,engine)

def generate_indexes4table(index_name:str, schema:str, tablename:str, column:str, db_params:dict):
    conn = psycopg2.connect(
        dbname=db_params['db_name'], user=db_params['db_user'], password=db_params['db_password'], host=db_params['db_host'])
    conn.autocommit=True
    with conn.cursor() as cur: 
        cur.execute(f"""CREATE INDEX IF NOT EXISTS {index_name} ON "{schema}"."{tablename}"({column})""")
    conn.close()
    

