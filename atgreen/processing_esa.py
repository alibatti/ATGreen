from .basic import *
import rioxarray


def wcesa2raster(geometry, folder, save_raster:bool=True, filename:str='clippedArea'):
    
    """
    The function extract the information on World Cover from https://esa-worldcover.s3.eu-central-1.amazonaws.com as a raster
    
    -------------------------------------------------------  
    Parameters:
    
    geometry: geometry to clip in CRS:4326
    folder: gdf of urban center database with cities geometry 
    filename: raster filename
    
    -------------------------------------------------------  
        
    Return: 
    raster
    """
        
    #Identify tiles in 'macro' grid intersecting with city boundary
    s3_url_prefix="https://esa-worldcover.s3.eu-central-1.amazonaws.com"
    url = f'{s3_url_prefix}/v100/2020/esa_worldcover_2020_grid.geojson'
    grid = gpd.read_file(url)
    tiles = grid[grid.intersects(geometry[0])]
 
    
    if save_raster==True:
        #For each intercting tile, extract the data and transform them in GeoDataFrame of points
        for ntile,tile in enumerate(tiles.ll_tile):
            url = f"{s3_url_prefix}/v100/2020/map/ESA_WorldCover_10m_2020_v100_{tile}_Map.tif"
            xds = rioxarray.open_rasterio(url, 'r').rio.clip(geometries=geometry, crs='EPSG:4326', drop=True, from_disk=True)
            xds.name = "Land Cover"
            xds.rio.to_raster(
        f"{folder}/{filename}_ntile_{ntile}.tiff",
        tiled=True,  # GDAL: By default striped TIFF files are created. This option can be used to force creation of tiled TIFF files.
        windowed=True,  # rioxarray: read & write one window at a time
            )
        return tiles


