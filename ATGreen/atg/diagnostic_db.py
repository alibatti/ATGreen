from .basic import *
import subprocess as sp
from sqlalchemy import create_engine

    
def intable_diagnostic(items_to_check:list, table:str, schema:str, column:str, db_params:dict, verbose:bool=False):
    
    """
    Check that all items in the list appear in the the column of the provided table in the database.
    
    -------------------------------------------------------    
    
    Parameters: 
    items_to_check: list of items, named as appearing in the database
    table: name of the table on the database
    schema: schema where the table is stored
    column: column of the table in the database
    db_params: dictionary with access specifics for the database
    verbose: boolean for whether to print diagnostic message or not
    
    ------------------------------------------------------- 
    
    Return:
    Diagnostic code: 0: no error; negative value: error
    Print diagnostic message
    
    
    """
    
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")       

    sql =f"""
    SELECT DISTINCT {column} 
    FROM {schema}.{table}
    """ 
    
    items_in_db=[f.replace('.tiff', '')[:f.find('_ntile')] if f.find('_ntile')!=-1 else  f.replace('.tiff', '') for f in list(pd.read_sql(sql,engine)[column])]
    engine.dispose()
    
    if len(set(items_to_check)-set(items_in_db))>0 or len(set(items_in_db)-set(items_to_check))>0:
        if verbose==True:
            print('Diagnostic completed. Error found. At least one city not in table.')
        not_in_list=list(set(items_in_db)-set(items_to_check))
        not_in_db=list(set(items_to_check)-set(items_in_db))
        return [-1, not_in_db, not_in_list]
    else:
        if verbose==True:
            print('Diagnostic completed. No error found')
        return [0]
    
    
    
def inschema_diagnostic(table:str, schema:str, db_params:dict, verbose:bool=False):
    
    """
    Check that given table exists in the schema.
    
    -------------------------------------------------------    
    
    Parameters: 
    table: table whose existence must be check
    schema: schema where table show be stored
    db_params: dictionary with access specifics for the database
    verbose: boolean for whether to print diagnostic message or not
    
    ------------------------------------------------------- 
    
    Return:
    Diagnostic code: 0: no error; negative value: error
    Print diagnostic message
    
    
    """
    
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")       

    sql =f""" 
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '{schema}'
    """ 
    
    tables=list(pd.read_sql(sql,engine)['table_name'])
    engine.dispose()
    
    if table not in tables:
        if verbose==True:
            print(f"Diagnostic completed. Error found. table {table} not in schema {schema}.")
        return -1
    else:
        if verbose==True:
            print('Diagnostic completed. No error found.')
        return 0

def itemsfromtable_diagnostic(column:str, table:str, schema:str, db_params:dict, col_filter:str='', values_filter:list=[]):
    
    """
    Extract selected column from table, applying filter if necessary
    
    -------------------------------------------------------    
    
    Parameters: 
    column: column to be extracted
    table: table in the database
    schema: schema of the table in the database
    db_params: dictionary with access specifics for the database
    col_filter: column for data filter
    values_filter: values of col_filter to be extracted
    
    ------------------------------------------------------- 
    
    Return:
    list
    
    """

    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")       

    if  len(values_filter)==0:
        sql =f"""
        SELECT {column}
        FROM {schema}."{table}"
        """ 
        
    elif len(values_filter)==1:
        sql =f"""
            SELECT DISTINCT city
            FROM {schema}."{table}"
            WHERE {col_filter}={values_filter[0]}
            """ 
    else:
        sql =f"""
            SELECT DISTINCT city
            FROM {schema}."{table}"
            WHERE {col_filter} IN {tuple(values_filter)}
            """ 
    col=list(pd.read_sql(sql,engine)[column])
    engine.dispose()
    return col


def osmextract_diagnostic(osm_extract:str, verbose:bool=False):
    
    """
    Check completeness of osm extract.
    
    -------------------------------------------------------    
    
    Parameters: 
    osm_extract: osm extract to check
    verbose: boolean for whether to print diagnostic message or not
    
    ------------------------------------------------------- 
    
    Return:
    Diagnostic code: 0: no error; negative value: error
    Print diagnostic message
    
    """
    
    if os.path.exists(osm_extract)==False:
        if verbose==True:
            print(f"Diagnostic completed. Error found. osm.pbf file not found.")
        return -1
    else:
        res=sp.getoutput(f"osmium check-refs -v -r {osm_extract}")
        if res.find('PBF error: truncated data (EOF encountered)')>0:
            if verbose==True:
                print(f"Diagnostic completed. Error found. osm.pbf file not correctly extracted.")
            return -2
        else:
            n_nodes=int(res[res.find('There are ')+10])
            n_ways=int(res[res.find('nodes, ')+7])
            n_rel=int(res[res.find('ways, and ')+10])
            if n_nodes*n_ways*n_rel==0:
                if verbose==True:
                    print(f"Diagnostic completed. Error found. osm.pbf file not correctly extracted.")
                return -2
    if verbose==True:
        print('Diagnostic completed. No error found.')  
    return 0 
    
    
### Distance matrices files
def distances_diagnostic(name_area:str, verbose:bool=False):
    
    """
    Check that computed distances for the selected area satisfy the following:
    - Street-network distance is no much (5 minutes) shorter than geodesic distance (notice 'a bit shorter is possible because the closest street to the centroid doesn't necessary go through it)
    - Same cells should have a 0 distance
    - At least one street-network distance is non-missing
    
    -------------------------------------------------------    
    
    Parameters: 
    name_area: name of the area as appearing in the database
    verbose: boolean for whether to print diagnostic message or not
    
    ------------------------------------------------------- 
    
    Return:
    Diagnostic code: 0: no error; negative value: error
    Print diagnostic message
    
    """
    
    engine=create_engine(f"postgresql+psycopg2://{db_params['db_user']}:{db_params['db_password']}@{db_params['db_host']}:{db_params['db_port']}/{db_params['db_name']}")       

    sql =f"""
        SELECT * 
        FROM distances."{name_area}"
    """ 
    df=pd.read_sql(sql,engine)
    engine.dispose()

    if len(df[(df['walk_minutes'].isnull()==False) & ((df['geodesic_minutes']-df['walk_minutes'])>5)])>0:
        if verbose==True:
            print(f"Diagnostic completed. Error found. At least one street-network distance is longer than geodesic distance.")
        return -1
    elif len(df[(df['walk_minutes'].isnull()==False) & (df['x_source']==df['x_dest']) & (df['y_source']==df['y_dest']) & (df['walk_minutes']>0)])>0:
        if verbose==True:
            print(f"Diagnostic completed. Error found. At least one 'same-cell-distance' different from 0.")
        return -2
    elif len(df[(df['walk_minutes'].isnull()==False)])==0:
        if verbose==True:
            print(f"Diagnostic completed. Error found. All street-network distances are missing.")
        return -3
    if verbose==True:
        print("Diagnostic completed. No error found.") 
    return 0

def queryDistancesWithLimit(city:str , which_distances:str, db_params: dict, limit:int):
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
        SELECT x_source, y_source, x_dest, y_dest, {dist_dict[which_distances]} as dist
        FROM distances."{city}"
        LIMIT {str(limit)}
        """  
    df=pd.read_sql(sql, engine)
    engine.dispose()
    
    return df
