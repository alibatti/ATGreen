#Import standard libraries needed for the Data Processing and Cleaning
from .basic import *
from shapely.geometry import Polygon, Point, LineString, MultiPolygon, shape, mapping
from shapely.ops import linemerge, polygonize
import osmium


""" Class to extract from the osm.pbf file """

class CounterHandler(osmium.SimpleHandler):
    def __init__(self):
        osmium.SimpleHandler.__init__(self)
        self.num_nodes = 0

    def node(self, n):
        self.num_nodes += 1
    
                         
""" Method to extract way based on key:value pairs """
class WayFindFeature(osmium.SimpleHandler):
    def __init__(self, features):
        osmium.SimpleHandler.__init__(self)
        self.ways = []
        self.key = []
        self.value = []
        self.way_id = []
        self.access = []
        self.name = []
        self.features = features

    def way(self, w):
        """ 
        Scan the osm-pbf file and extract ways with key:value pairs. 
        Ways are treated as Polygon - if first node is identical to the last node - 
        or LineString - if first node is different from last node.
        Stored attributes for each way are: 
        - osm key
        - osm value
        - osm id
        - tag: access
        - tag: name
        - geometry
        """
        for key in self.features.keys():
            if (key in w.tags and w.tags[key] in self.features[key]):
                nodes = []
                self.key.append(key)
                self.value.append(w.tags[key])
                self.way_id.append(w.id)
                if 'access' in w.tags:
                    self.access.append(w.tags['access'])
                else:
                    self.access.append('')
                if 'name' in w.tags:
                    self.name.append(w.tags['name'])
                else:
                    self.name.append('')
                for n in w.nodes:  
                    x = osmium.geom.Coordinates(n.location).x
                    y = osmium.geom.Coordinates(n.location).y
                    nodes.append(Point(x,y))
                if len(nodes)==1:
                    self.ways.append(nodes[0])
                elif nodes[0]==nodes[-1]:
                    self.ways.append(Polygon(nodes))
                else:
                    self.ways.append(LineString(nodes))

""" Extract way based on id  """                 
class WayFind(osmium.SimpleHandler):
    def __init__(self, id_find):
        osmium.SimpleHandler.__init__(self)
        self.ways = []
        self.ways_id = []
        self.id_find=id_find

    def way(self, w):
        """ 
          Scan the osm-pbf file and extract ways with specific osm id. 
          This is used to reconstruct the geometry of relations (by extracting all relation members)
          Ways are treated as Polygon - if first node is identical to the last node - 
          or LineString - if first node is different from last node.
          Stored attributes for each way are: 
          - osm id
          - geometry
        """
        for i in self.id_find:
            if w.id==i: 
                self.ways_id.append(w.id)
                nodes = []
                for n in w.nodes:  
                    x = osmium.geom.Coordinates(n.location).x
                    y = osmium.geom.Coordinates(n.location).y
                    nodes.append(Point(x,y))
                if nodes[0]==nodes[-1]:
                    self.ways.append(Polygon(nodes))
                else:
                    self.ways.append(LineString(nodes))


""" Find relations based on [key:value] pairs """
class RelationFindFeature(osmium.SimpleHandler):
    def __init__(self, features):
        osmium.SimpleHandler.__init__(self)
        self.key = []
        self.value = []
        self.name = []
        self.relation_id = []
        self.way_ref = []
        self.way_type = []
        self.way_role = []
        self.features = features

    def relation(self, r): 
        """ 
          Scan the osm-pbf file and extract relations with specific key:value pairs and extract the full member list for each relation. 
          We restrict to multipolygon for the nature of our project. type:MultiPolygon implies that area-building rules can be used on the members.
          For each member of the relation we extract: 
          - Relation-level tags
              - relation id
              - relation name
              - relation key
              - relation value
          - Member-level tags
              - way type
              - way role
              - way reference id
        """
        for key in self.features.keys():
            if (key in r.tags and 'type' in r.tags):
                if (r.tags[key] in self.features[key] and r.tags['type']=='multipolygon'):
                    for w in r.members:
                        self.relation_id.append(r.id)
                        if 'name' in r.tags:
                            self.name.append(r.tags['name'])
                        else:
                            self.name.append('')
                        self.key.append(key)
                        self.value.append(r.tags[key])
                        self.way_ref.append(w.ref)
                        self.way_type.append(w.type)
                        self.way_role.append(w.role)
                        

                                                
""" Define function to associate a geometry to each relation, based on the geometry of its members """

def get_geometry_one_rel(rel:str, relations_gdf:gpd.geodataframe):
    
    """ 
    Reconstruct geometry of a relation, based on its member-level information.
    The information on how to assemble a MultiPolygon from its component LineStrings and Polygons, is available here: https://wiki.openstreetmap.org/wiki/Relation:multipolygon/Algorithm.
    
    -------------------------------------------------------  
        
    Parameters:
    
    rel: osm relation id
    relations_gdf: geopandas.GeoDataFrame with a list of members for the relation of interest. 
                   The dataframe must have the following columns:
                   'rel_id': store the OSM relation id 
                   'geometry': geometry of the member. It can be either a linestring or a polygon
                   'way_role': role of the member in the relation
                   
    -------------------------------------------------------  
                   
    Description:
    
    Step 1: Generate lists of members based on their role (inner/outer) and their topology (LineString/Polygon)
    Step 2: Use shapely Linemerge on the list of LineStrings (from STEP 1), to identifying overlapping linestring and reconstruct unique geometries. If the merge LineString are not LineRings (first node!= last node), close them (important in case the initial osm-pbf extraction removed members). Finally polygonize the resulting geometries and add to the list of inner/outer polygons. 
    Step 3: Loop through the outer polygons subtracting the inner polygons and appending to the list of final geometries.
    Step 4: Define the final Polygon or MultiPolygon (based on the number of reconstructed geometries from Step 3)
    
    -------------------------------------------------------  
    
    Return: 
    shapely.geometry
    """
    
    #Step 1:
    outer_linestrings=relations_gdf[(relations_gdf['rel_id']==rel) & (relations_gdf.geometry.type=='LineString') & (relations_gdf.way_role=='outer')].geometry.to_list()
    inner_linestrings=relations_gdf[(relations_gdf['rel_id']==rel) & (relations_gdf.geometry.type=='LineString') & (relations_gdf.way_role=='inner')].geometry.to_list()
    outer_polygons=relations_gdf[(relations_gdf['rel_id']==rel) & (relations_gdf.geometry.type=='Polygon') & (relations_gdf.way_role=='outer') ].geometry.to_list()
    inner_polygons=relations_gdf[(relations_gdf['rel_id']==rel) & (relations_gdf.geometry.type=='Polygon') & (relations_gdf.way_role=='inner')].geometry.to_list()
    
    #Step 2:
    merged_outer_linestrings = linemerge(outer_linestrings)
    merged_inner_linestrings = linemerge(inner_linestrings)
    
    # polygonize each linestring separately and append to list of outer polygons
    if merged_outer_linestrings.geom_type == "LineString": 
        if merged_outer_linestrings.coords.xy[0] != merged_outer_linestrings.coords.xy[-1]:
            merged_outer_linestrings=linemerge([merged_outer_linestrings, LineString([(merged_outer_linestrings.coords.xy[0][0], merged_outer_linestrings.coords.xy[1][0]), (merged_outer_linestrings.coords.xy[0][-1], merged_outer_linestrings.coords.xy[1][-1])])])
        outer_polygons+=list(polygonize(merged_outer_linestrings))
    elif merged_outer_linestrings.geom_type == "MultiLineString":
        for merged_outer_linestring in list(merged_outer_linestrings.geoms):
            if merged_outer_linestring.coords.xy[0] != merged_outer_linestring.coords.xy[-1]:
                merged_outer_linestring=linemerge([merged_outer_linestring, LineString([(merged_outer_linestring.coords.xy[0][0], merged_outer_linestring.coords.xy[1][0]), (merged_outer_linestring.coords.xy[0][-1], merged_outer_linestring.coords.xy[1][-1])])])
            outer_polygons+=list(polygonize(merged_outer_linestring))
    if merged_inner_linestrings.geom_type == "LineString":
        if merged_inner_linestrings.coords.xy[0] != merged_inner_linestrings.coords.xy[-1]:
            merged_inner_linestrings=linemerge([merged_inner_linestrings, LineString([(merged_inner_linestrings.coords.xy[0][0], merged_inner_linestrings.coords.xy[1][0]), (merged_inner_linestrings.coords.xy[0][-1], merged_inner_linestrings.coords.xy[1][-1])])])
        inner_polygons+=list(polygonize(merged_inner_linestrings))
    elif merged_inner_linestrings.geom_type == "MultiLineString":
        for merged_inner_linestring in list(merged_inner_linestrings.geoms):
            if merged_inner_linestring.coords.xy[0] != merged_inner_linestring.coords.xy[-1]:
                merged_inner_linestring=linemerge([merged_inner_linestring, LineString([(merged_inner_linestring.coords.xy[0][0], merged_inner_linestring.coords.xy[1][0]), (merged_inner_linestring.coords.xy[0][-1], merged_inner_linestring.coords.xy[1][-1])])])
            inner_polygons+=list(polygonize(merged_inner_linestring))
    
    #Step 3:
    final_geom = []

    for outer_polygon in outer_polygons:
        for inner_polygon in inner_polygons:
            if inner_polygon.within(outer_polygon):
                    outer_polygon = outer_polygon.buffer(0).difference(inner_polygon.buffer(0))

        if outer_polygon.geom_type == "Polygon":
            final_geom.append(outer_polygon)
        elif outer_polygon.geom_type == "MultiPolygon":
            final_geom.extend(list(outer_polygon.geoms))
    
    # Step 4
    if len(final_geom) == 1:
        geometry = final_geom[0]
    else:
        geometry = MultiPolygon(final_geom)
    
    return geometry


def generate_relation_geom(relations_gdf:gpd.geodataframe, filename:str):
    
    """ 
    General function to be applied to the geopandas.GeoDataFrame with info on relation, to reconstruct the geometry of each relation.
    The function call get_geometry_one_rel() previously defined.
    
    -------------------------------------------------------  
        
    Parameters:
    relations_gdf: geopandas.GeoDataFrame with a list of members for the relation of interest. 
                   The dataframe must have the following columns:
                   'rel_id': store the OSM relation id 
                   'rel_value': store the OSM relation value
                   'rel_key': store the OSM relation key
                   'rel_name': store the OSM relation name
                   'way_id': id of the member. It can be either a linestring or a polygon
                   'way_role': role of the member in the relation
    filename: osm.pbf source file 
    -------------------------------------------------------  

    Description:
    
    Step 1: Call WayFind to extract the geometry of all relations members in your GeoDataFrame from your osm.pbf extract. Store them in a geopandas.GeoDataFrame. Merge the geometry into the original geopandas.GeoDataFrame. Drop members with no geometry (if member is outside of extract for instance).
    Step 2: For each relation, call get_geometry_one_rel to reconstruct the geometry. Store the other relation-level information. 
    Step 3: Generate final geopandas.GeoDataFrame. Buffer the geometries to make them valid (with 0 buffer). Add column 'osm_element' specifying that the element is a relation.
    
    -------------------------------------------------------  
    
    Return:
    geopandas.GeoDataFrame with OSM relations and their reconstructed geometry
    """
    
    #Step 1: 
    relations_ways = WayFind(relations_gdf.way_id.to_list())
    relations_ways.apply_file(filename, locations = True,idx='flex_mem' )
    relations_ways_gdf = gpd.GeoDataFrame( {'way_id': relations_ways.ways_id, 'geometry': relations_ways.ways}, crs='EPSG:4326', geometry='geometry')

    #merge geometries
    relations_gdf=pd.merge(relations_ways_gdf, relations_gdf, on='way_id', how='right')
    relations_gdf=relations_gdf[(relations_gdf.geometry.is_empty==False) & (relations_gdf.geometry!=None)]
    
    #Step 2: 
    rel_id=[]
    rel_value=[]
    rel_key=[]
    rel_name=[]
    rel_geometry=[]
    for rel in relations_gdf['rel_id'].unique():
        rel_id.append(rel)
        rel_value.append(relations_gdf[relations_gdf['rel_id']==rel]['osm_value'].values[0])
        rel_key.append(relations_gdf[relations_gdf['rel_id']==rel]['osm_key'].values[0])
        rel_name.append(relations_gdf[relations_gdf['rel_id']==rel]['osm_name'].values[0])
        # Call get_geometry_one_rel to reconstruct the geometry
        rel_geometry.append(get_geometry_one_rel(rel, relations_gdf))
    
    #Step 3: 
    final=gpd.GeoDataFrame({'osm_id':rel_id,'osm_value':rel_value, 'osm_key':rel_key, 'osm_name':rel_name, 'geometry':rel_geometry }, geometry='geometry', crs='EPSG:4326')
    # Buffer the geometries with a 0 buffer to make them valid
    final['geometry']=final['geometry'].buffer(0)
    final['osm_element']='relation'
    
    return final

def waysExtraction(filename:str, features:dict, drop_private:bool=True, drop_linestring:bool=True): 
    
    """ 
    Pipeline to extract ways with specific key:value pairs
    
    -------------------------------------------------------  
        
    Parameters:
    
    filename: osm.pbf source file 
    features: dictionary of key-value pairs
    drop_private: if we want to drop elements with tags 'access' in ['no', 'private']
    drop_linestring: if we want to drop linestring element (open ways)
    
    -------------------------------------------------------  
                  
    Return: geopandas.GeoDataFrame
    """


    ways = WayFindFeature(features)
    ways.apply_file(filename,locations = True,idx='flex_mem' )
    #create gdf with the geometry of the selected ways
    ways_osm = gpd.GeoDataFrame( {'geometry': ways.ways,'osm_key': ways.key ,'osm_value': ways.value, 'access':ways.access, 'osm_name':ways.name, 'osm_id':ways.way_id}, crs='EPSG:4326')
    ways_osm=ways_osm[ways_osm.geometry.type!=Point]
    #Drop if access is forbidden
    if drop_private==True:
        ways_osm=ways_osm[~ways_osm.access.isin(['no', 'private'])].drop(columns=['access'])
    gdf=ways_osm.copy()
    if drop_linestring==True:
        gdf=gdf[gdf.geometry.type!='LineString']
    gdf['osm_element']='way'
    
    return gdf

def relationsExtraction(filename:str,  features:dict, drop_private:bool=True, drop_linestring:bool=True):
    
    """
    Pipeline to extract ways with specific key:value pairs
    -------------------------------------------------------  
        
    Parameters:
    filename: osm.pbf source file 
    features: dictionary of key-value pairs
    drop_private: if we want to drop elements with tags 'access' in ['no', 'private']
    drop_linestring: if we want to drop linestring element (open ways)
    
    -------------------------------------------------------  
    
    Return:
    geopandas.GeoDataFrame
    """
    
    #Extract relations with relevant tags:
    relations = RelationFindFeature(features)
    relations.apply_file(filename,locations = True,idx='flex_mem' )

    #create gdf with the geometry of the selected ways
    relations_gdf = gpd.GeoDataFrame( {'rel_id': relations.relation_id, 'osm_key': relations.key, 'osm_value': relations.value, 'way_id': relations.way_ref,'way_role': relations.way_role,'way_type': relations.way_type ,'osm_name': relations.name})

    if len(relations_gdf)>0:
        gdf=generate_relation_geom(relations_gdf, filename)
        return gdf
    else:
        print('No relations for selected features')
        gdf=gpd.GeoDataFrame()
        return gdf


