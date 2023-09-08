<p>The repository provides the material to reproduce the study "On the need to move from a single indicator to a multi-dimensional framework to measure accessibility to urban green" by A. Battiston and R. Schifanella. <br>
<a href="https://arxiv.org/abs/2308.05538">here</a>
Pre-print available <a href="https://arxiv.org/abs/2308.05538">here</a>.  <br>
Explore the results of the study through our <a href="http://atgreen.hpc4ai.unito.it/">interactive web interface</a>. <p>

### Abstract
<p> With the recent expansion of urban greening interventions, the definition of spatial indicators to measure the provision of urban greenery has become of pivotal importance in informing the policy-design process. By analyzing the stability of the population and area rankings induced by several indicators of green accessibility for over 1,000 cities worldwide, we investigate the extent to which the use of a single metric provides a reliable assessment of green accessibility in a city. The results suggest that, due to the complex interaction between the spatial distribution of greenspaces in an urban center and its population distribution, the use of a single indicator might lead to insufficient discrimination across areas or subgroups of the population, even when focusing on one form of green accessibility. From a policy perspective, this indicates the need to switch toward a multi-dimensional framework that is able to organically evaluate a range of indicators at once. <p>

### Structure of the repository

#### atgreen
<p> Python functions for the computation of the green accessibility indices. <p>  

#### example
<p> This directory contains the Jupyter Notebooks required to set up a green accessibility database mimicking the one used for the study. <br>
The notebooks should be run in sequential order and require the installation of the functions contained in the <b>atgreen</b> directory. <br>
For the computation of the street-network distances, we make use of the matrix computation capabilities of <a href="https://github.com/Project-OSRM/osrm-backend">Open Source Routing Machine (OSRM)</a> with its NodeJS Bindings. For a tutorial, please follow the instruction <a href="https://gis-ops.com/osrm-nodejs-bindings/">here</a> and paste the file at <b>resources_computation_distances</b> in the associated directory. <p>

<ul>
  <li> 00.01.01_DataSetup_DirectoriesSetup </li> Setup project-related directories. <br>  Inputs: PATH= main project directory </li> 
  <li> 00.01.02_DataSetup_DataBaseSetup </li>  Setup project-related postgreSQL database. <br>  Inputs: PATH= main project directory; DB_NAME= database name; DB_USER= database user; DB_PASSWORD= database password; DB_HOST= database host; DB_PORT= database port.     </li>
  <li> 00.01.03_DataSetup_Define_dictionaries </li>  Define a set of dictionaries to be used throughout the data processing. Upload them to the database. <br>  Inputs: PATH= main project directory.    </li>
  <li> 00.02.01_DataProcessing_CITIES_BOUNDARY </li>   Upload the boundaries of the cities in the sample to the database. Save a unique shapefile in the PATH/boundary/ directory. <br>  Inputs: PATH= main project directory.  </li>
  <li> 00.03.01_DataProcessing_GHS_POP_data </li>   Get the population grid from a raster file. <br>  Inputs: PATH= main project directory; GRID_FILE= Source file for population data; GRID_CRS= Coordinate reference system of the source file; BUFFER_SIZE= size of the buffer to build around the city boundary (to be provided in meters).  </li>
  <li> 00.04.01_DataProcessing_OSMData_UCExtraction </li>   Generate city-level extracts of OSM data  <br>  Inputs: PATH= main project directory; OSM_INPUT_FILE= Source file for OpenStreetMap data.  </li>
  <li> 00.04.02_DataProcessing_OSMData_GreenFeaturesExtraction </li>  Extraction of green features from OSM data and upload to the database. To customize the extracted features, please amend the dictionaries defined at <b>00.01.03_DataSetup_Define_dictionaries<b> <br>  Inputs: PATH= main project directory.  </li>
  <li> 00.04.03_DataProcessing_OSMData_Remapping </li>   Remapping of green features (several combinations) to population grid. Upload information to the database <br>  Inputs: PATH= main project directory.  </li>
 <li> 00.05.01_DataProcessing_DistanceMatrices_Queries </li>   Computation of distance matrix of each city. <br>  Inputs: PATH= main project directory; 
CELL_BUFFER=buffer around each cell within which to compute distances (provided in meters); ROUTING_PROFILE= routing profile (ex: "foot"); OSRM_WORKING_FOLDER: OSRM directory; OSM_INPUT_FOLDER_CITIES=Input folder for city-level extract of OSM data </li>
 <li> 00.05.02_DataProcessing_DistanceMatrices_LoadOnDB </li>   Upload distance matrices to the database in the form of single edge lists (one for each city). <br>  Inputs: PATH= main project directory. </li>
   <li> 00.06.01_DataProcessing_ESAData_ExtractionAndLoadOnDB </li>    Download the city-level extract of the WC-ESA 2020 data. <br>  Inputs: PATH= main project directory; BUFFER_SIZE= size of the buffer to build around the city boundary (to be provided in meters). </li>
    <li> 00.06.02_DataProcessing_ESAData_Remapping </li>   Remapping of ESA data to the population grid and upload of the results to the database <br>  Inputs: PATH= main project directory. </li>
  <li> 00.07.01_DataProcessing_GenerateSQLindexes </li>   Generation of table indices on the database. <br>  Inputs: PATH= main project directory. </li>
</ul>

#### Data sources
All data sources used for this project are publicly available. 

##### City boundary
Cities were defined according to the boundaries in the Urban Centre Database of the Global Human Settlement 2015, revised version R2019A (GHS-UCDB). Out of 13,000 urban centers recorded in the database, we retained the most populated 50 UCs per country, provided that they had at least 100,000 inhabitants. We further excluded cities whose quality for which the quality of the OpenStreetMap (OSM) data was deemed insufficient according to the procedure described in the manuscript. The final sample comprised 1,040 UCs across 145 countries.

##### Population data
Population data were extracted from the population grid of the Global Human Settlement 2015 (revised version 2019A) at a 9-arcsecond resolution. The data consist of residential population estimates for the year 2015, disaggregated from census or administrative units to grid cells and informed by the distribution and density of built-up as mapped in the corresponding Global Human Settlement Layer (GHSL) global layer.

##### distance matrices
The problem of computing the walking distances between residential areas and green spaces in each urban area was simplified to the computation of walking distances between the centroids of the base grid, i.e. of computing a walking origin-destination matrix. To this scope, we locally installed the routing engine Open Source Routing Machine (OSRM) and used street-network data from the local OSM dumps.

##### greenspaces and other green features
Greenspaces and other green features were extracted from two different data sources. For the minimum distance and per-person indexes, where we are interested in measuring the availability of public and accessible greenspaces, we used information from <a href="https://www.openstreetmap.org/#map=12/53.3575/-1.5056">OpenStreetMap</a>. For the exposure index, where we are interested in measuring exposure to green features, regardless of their use, we used information form on land coverage from the <a href="https://worldcover2020.esa.int/">World Coverage 2020 of the European Space Agency</a>.





 
