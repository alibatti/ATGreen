from .basic import *
import rioxarray
import rasterio

def getClippedRaster(val_name:str, filename:str, geometry_to_clip:gpd.geoseries.GeoSeries, return_raster:bool=True, save_raster:bool=True, folder:str="./", clean_name:str='clippedArea'):
    
    """   
    Save raster file 
    -------------------------------------------------------  
    
    Parameters:
    
    val_name: name of extracted value
    filename: filename original raster
    geometry_to_clip: geopandas.geoseries.GeoSeries of geometries to use for clipping in EPSG:4326
    return_raster: boolean, True if the raster is returned. 
    save_raster: boolean, True if the raster is saved. 
    folder: folder to save the raster in tiff format
    clean_name: raster name for saved raster
    
    -------------------------------------------------------  
    
    Return: save raster in folder
    """

    xds = rioxarray.open_rasterio(filename, 'r').rio.clip(geometries=geometry_to_clip, crs='EPSG:4326', drop=True, from_disk=True)        
    xds.name = val_name
    
    if save_raster==True:
        xds.rio.to_raster(
        f"{folder}/{clean_name}.tiff",
        tiled=True,  # GDAL: By default striped TIFF files are created. This option can be used to force creation of tiled TIFF files.
        windowed=True,  # rioxarray: read & write one window at a time
        )  
    if return_raster==True:
        return xds   
    
