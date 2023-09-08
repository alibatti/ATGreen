<p>The repository provides the material to reproduce the study "On the need to move from a single indicator to a multi-dimensional framework to measure accessibility to urban green" by A. Battiston and R. Schifanella. <br>
<a href="https://arxiv.org/abs/2308.05538">here</a>
Pre-print available <a href="https://arxiv.org/abs/2308.05538">here</a>.  <br>
Explore the results through our <a href="http://atgreen.hpc4ai.unito.it/">interactive web interface</a>. <p>


## Abstract
<p> With the recent expansion of urban greening interventions, the definition of spatial indicators to measure the provision of urban greenery has become of pivotal importance in informing the policy-design process. By analyzing the stability of the population and area rankings induced by several indicators of green accessibility for over 1,000 cities worldwide, we investigate the extent to which the use of a single metric provides a reliable assessment of green accessibility in a city. The results suggest that, due to the complex interaction between the spatial distribution of greenspaces in an urban center and its population distribution, the use of a single indicator might lead to insufficient discrimination across areas or subgroups of the population, even when focusing on one form of green accessibility. From a policy perspective, this indicates the need to switch toward a multi-dimensional framework that is able to organically evaluate a range of indicators at once. <p>

## Structure of the repository

### atgreen
<p> Python functions for the computation of the green accessibility indices. <p>  

### example
<p> This directory contains the Jupyter Notebooks required to set up a green accessibility database mimicking the one used for the study. <br>
The notebooks should be run in sequential order and require the installation of the functions contained in the <b>atgreen</b> directory. <br>
For the computation of the street-network distances, we make use of the matrix computation capabilities of <a href="https://github.com/Project-OSRM/osrm-backend">Open Source Routing Machine</a> with its NodeJS Bindings. For a tutorial, please follow the instruction <a href="https://gis-ops.com/osrm-nodejs-bindings/">here</a> and paste the file at <b>resources_computation_distances</b> in the associated directory. <p>

<ul>
  <li> 00.01.01_DataSetup_DirectoriesSetup </li> Setup project-related directories. Input: PATH= main project directory </li>
  <li> 00.01.02_DataSetup_DataBaseSetup </li> Setup project-related postgreSQL database. Input: PATH= main project directory; DB_NAME= database name; DB_USER= database user; ; DB_PASSWORD= database password; DB_HOST= database host; DB_PORT= database port.     </li>
  <li> 00.01.03_DataSetup_Define_dictionaries </li> Define a set of dictionaries to be used throughout the data processing. Upload them to the database. Input: PATH= main project directory.    </li>
  <li> 00.02.01_DataProcessing_CITIES_BOUNDARY </li> Upload the boundaries of the cities in the sample to the database. Save a unique shapefile in the PATH/boundary/ directory. Input: PATH= main project directory.  </li>
  <li> 00.03.01_DataProcessing_GHS_POP_data </li> Get the population grid from a raster file. Input: PATH= main project directory.  </li>
</ul>

## Data sources







 
