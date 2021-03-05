## Changes in 0.7.0

* Fixed a bug that occurred when writing datasets obtained from Sentinel Hub to NetCDF file.
  In this case an error 
  `TypeError: Invalid value for attr 'processing_level': None must be a number, a string, an ndarray or a list/tuple of numbers/strings for serialization to netCDF files`
  was raised. (#44)
* Cube configuration parameter `time_range` accepts `None` as start and end date values. For example,
  if `time_range=[None,'2021-02-01']` is provided, then the start date is '1970-01-01'; 
  if `time_range=['2021-02-01', None]` is provided, then the end date is the current date (today). 
* Changed the cube configuration parameter `crs` (spatial coordinate reference system)
  so users can pass `"WGS84"`, `"CRS84"` or `"EPSG:{code}"`, where `{code}` is an EPSG 
  code e.g. `"EPSG:4326"`. The old URI notation is still supported, e.g.
  `"http://www.opengis.net/def/crs/EPSG/0/4326"`.
* Fixed a bug that caused all band attributes to be included in each band's attributes.
  This resulted in three times larger JSON metadata exports.  
* Fixed a bug in coordinate metadata: value of latitude `long_name` attribute should be 
  `"latitude"` (thanks to Matthew Fung / @mattfung).
* Implemented error handling policy when Sentinel Hub API returns an HTTP error status 
  for a requested data chunk (#39). 
  Behaviour is controlled by the `error_policy` argument:
  - for value `"fail"`, a `SentinelHubError` is raised with complete error information;
  - for value `"warn"`, a warning is issued with complete error information, and a 
    fill-value chunk is generated.
* Removed module `xcube_sh.geodb` entirely. Also delete example notebook 
  `Demo1-xcube-sh.ipynb` as it referred to the removed package. Its other content 
  is covered by remaining notebooks.

## Changes in 0.6.2

* Fixed problem with the encoding of a dataset's coordinate variables that occurs 
  when using `xcube_xh` with xcube 0.6.1. (#27)

* Fixed issue with `crs` parameter in cube configuration. 
  If `crs='http://www.opengis.net/def/crs/OGC/1.3/CRS84'` was used, `x`, `y` coordinate 
  variables where produced instead of expected `lon`, `lat`. (#26)

## Changes in 0.6.1

* Fixed following issues regarding datasets' band metadata:
  - Renamed misspelled band attribute `bandwith` into `bandwidth`.
  - Values of band attributes `wavelength` and `bandwidth`, if any, are now 
    always numeric. For Sentinel A and B datasets, values of `wavelength` and `bandwidth` 
    are averages.
    Individual wavelengths and bandwidths for Sentinel A and B, if any, are available 
    in attributes `wavelength_a`, `wavelength_b` and `bandwidth_a`, `bandwidth_b`.    

* Removed outdated module `xcube_sh.geodb`, removed unused `geodb` folder, and added a 
  deprecation notice in Notebook `Ex4-DCFS-GeoDB.ipynb`. 

## Changes in 0.6.0

* Enhanced band metadata and added flag encodings for Sentinel-3 OCLI L1B datasets. 
  Additional band metadata and flag encodings for Sentinel-3 SLSTR L1B datasets will follow soon. 
   
* Now using SENTINEL Hub's Catalogue API instead of the WFS (#14)
  - to determine actual observations within given bounding box and time range;
  - to determine time range of available data collections.  

* The keyword argument `instance_id` of the `SentinelHub` and `SentinelHubStore` constructors
  has been deprecated. It is no longer required    

## Changes in 0.3.0.dev1
 
*Note, this version has been accidently released as v0.5.0.*

* Documented store open parameters. Store open parameter `variable_names` and  
  `CubeConfig` parameter `band_names` may now be `None`. In this case all 
  variables/bands are included. (#12)
* `CubeConfig` now accepts parameter `bbox` instead of parameter `geometry`.
  `geometry` is currently deprecated, but may be supported again later. 
  It may then also be a WKT Geometry string or GeoJSON object. (#4)
* `xcube-sh` now implements the new `xcube.core.store.DataStore` interface.
* Fixed `KeyError` exception when opening cube from dataset "DEM".

## Changes in 0.2

* Added support for BYOD (#1): To utilize, create new `CubeConfig` 
  with `dataset_name="CUSTOM"` and provide value for new 
  keyword argument `collection_id`.
* Added `CubeConfig` parameter `band_sample_type` which may be one
  of `"INT8"`, `"INT16"`, `"UINT8"`, `"UINT16"`, `"FLOAT32"`, `"FLOAT64"` or a sequence of those. (#2)   
* If `CubeConfig.time_period` is *not* given, we now use the SentinelHub WFS 
  to identify overpass time of scenes that intersect the given bounding box by 
  `CubeConfig.bbox`. (#3)  
* Renamed `xcube-dcfs` to `xcube-sh`. Made the `xcube-sh` environment dependent 
  on `xcube`. `SentinelhubStore` now inherits from `xcube.api.CubeStore`. (#4)  
* Added class SentinelOAuth2Session to allow proper pickling of the oauth session 
* The `ViewerServer` constructor will now try killing any running server on same port number, 
  before a new server instance is started. Style parsing has been fixed.
  Setting `server_url=None` will make it default to `http://localhost:8080`. 


## Changes in 0.1
 
Initial version for ESA Demo 1.
