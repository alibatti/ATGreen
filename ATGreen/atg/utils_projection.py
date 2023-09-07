from .basic import *
from pyproj import CRS

#Import standard libraries needed for the projection module
#########################################################################################
###                                   Projection utils                                ###
###                      Projection function from osmnx project:                      ###
###           https://github.com/gboeing/osmnx/blob/main/osmnx/projection.py          ###
##########################################################################################

def is_projected(crs):
    """
    Determine if a coordinate reference system is projected or not.
    This is a convenience wrapper around the pyproj.CRS.is_projected function.
    Parameters
    ----------
    crs : string or pyproj.CRS
        the coordinate reference system
    Returns
    -------
    projected : bool
        True if crs is projected, otherwise False
    """
    return CRS.from_user_input(crs).is_projected


def project_geometry(geometry, crs=None, to_crs=None, to_latlong=False):
    """
    Project a shapely geometry from its current CRS to another.
    If to_crs is None, project to the UTM CRS for the UTM zone in which the
    geometry's centroid lies. Otherwise project to the CRS defined by to_crs.
    Parameters
    ----------
    geometry : shapely.geometry.Polygon or shapely.geometry.MultiPolygon
        the geometry to project
    crs : string or pyproj.CRS
        the starting CRS of the passed-in geometry. if None, it will be set to
        settings.default_crs
    to_crs : string or pyproj.CRS
        if None, project to UTM zone in which geometry's centroid lies,
        otherwise project to this CRS
    to_latlong : bool
        if True, project to settings.default_crs and ignore to_crs
    Returns
    -------
    geometry_proj, crs : tuple
        the projected geometry and its new CRS
    """
    if crs is None:
        crs = settings.default_crs

    gdf = gpd.GeoDataFrame(geometry=[geometry], crs=crs)
    gdf_proj = project_gdf(gdf, to_crs=to_crs, to_latlong=to_latlong)
    geometry_proj = gdf_proj.geometry.iloc[0]
    return geometry_proj, gdf_proj.crs

def project_gdf(gdf, to_crs=None, to_latlong=False):
    """
    Project a GeoDataFrame from its current CRS to another.
    If to_crs is None, project to the UTM CRS for the UTM zone in which the
    GeoDataFrame's centroid lies. Otherwise project to the CRS defined by
    to_crs. The simple UTM zone calculation in this function works well for
    most latitudes, but may not work for some extreme northern locations like
    Svalbard or far northern Norway.
    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        the GeoDataFrame to be projected
    to_crs : string or pyproj.CRS
        if None, project to UTM zone in which gdf's centroid lies, otherwise
        project to this CRS
    to_latlong : bool
        if True, project to settings.default_crs and ignore to_crs
    Returns
    -------
    gdf_proj : geopandas.GeoDataFrame
        the projected GeoDataFrame
    """
    if gdf.crs is None or len(gdf) < 1:  # pragma: no cover
        raise ValueError("GeoDataFrame must have a valid CRS and cannot be empty")

    # if to_latlong is True, project the gdf to latlong
    if to_latlong:
        gdf_proj = gdf.to_crs(settings.default_crs)

    # else if to_crs was passed-in, project gdf to this CRS
    elif to_crs is not None:
        gdf_proj = gdf.to_crs(to_crs)

    # otherwise, automatically project the gdf to UTM
    else:
        if is_projected(gdf.crs):  # pragma: no cover
            raise ValueError("Geometry must be unprojected to calculate UTM zone")

        # calculate approximate longitude of centroid of union of all geometries in gdf
        avg_lng = gdf.geometry.representative_point().x.mean()

        # calculate UTM zone from avg longitude to define CRS to project to
        utm_zone = int(np.floor((avg_lng + 180) / 6) + 1)
        utm_crs = f"+proj=utm +zone={utm_zone} +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

        # project the GeoDataFrame to the UTM CRS
        gdf_proj = gdf.to_crs(utm_crs)

    return gdf_proj


