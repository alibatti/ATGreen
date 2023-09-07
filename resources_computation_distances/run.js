


// the nodejs bindings
const OSRM = require("osrm");
const { convertCSVToArray } = require('convert-csv-to-array');
const converter = require('convert-csv-to-array');

var coordinates = require('fs').readFileSync(process.argv[2],'utf8')
const array=convertCSVToArray(coordinates, {header:false, type:'array'});
// teaching the bindings to use the osrm graph prepared in the previous step
const osrm = new OSRM(process.argv[3]+".osrm");

const arr_radius = Array(array.length).fill(10000)

// a small function to create the osrm options
// https://github.com/Project-OSRM/osrm-backend/blob/master/docs/nodejs/api.md
const makeOsrmOptions = (sources, destinations) => {
  return {
    coordinates: array,
    sources: sources || [],
    destinations: destinations || [],
    annotations: ["duration"],
    radiuses:arr_radius }}

var durations = Array.from({ length: coordinates.length }).fill().map(item => ([]))
const osrmOptions = makeOsrmOptions()
osrm.table(osrmOptions, (err, result) => {
  if (err) {
    console.log(err);
  }
 var file = require('fs').createWriteStream(process.argv[4]);
 file.on('error', function(err) { /* error handling */ });
 result.durations.forEach(function(v) { file.write(v.join(', ') + '\n'); });
 file.end();
});
