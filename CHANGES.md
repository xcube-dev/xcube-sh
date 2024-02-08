## Changes in 0.10.4 (in development)

## Changes in 0.10.3

* Fixed a bug which caused that, when the `four_d` parameter was set to true
  when opening a dataset, the data of the bands of the resulting cubes could 
  not be accessed. (#101)

## Changes in 0.10.2

* Added support for Landsat-4,5 Level-2 (`"LTML2"`) 
  and Landsat 7 Level-2 (`"LETML2"`) collections available through the endpoint
  `api_url="https://services-uswest2.sentinel-hub.com"`.
* Adapted to work with Python 3.10
* Fixed default `time_tolerance` used when `time_period` is None. 
  It is now correctly set to ten minutes.

* A new parameter `extra_search_params` has been added the `CubeConfig` as 
  well as to the data store's `open_data()` method. (#93)

  The value of `extra_search_params` is a dictionary that defines 
  additional search parameters when querying the individual observations 
  (time slices) that will form the data cube.  
  Examples for such parameters are the keys `"filter"`, `"filter-lang"`,
  or ``"fields"``. More on this can be found in the 
  [Sentinel Hub Catalog API documentation](https://docs.sentinel-hub.com/api/latest/api/catalog/).

  Note that `extra_search_params` will be ignored if `time_period` is used
  (i.e., given and not `None`).

## Changes in 0.10.1

* Fixed problem with that some parameters that were listed as result from 
  `get_open_data_params_schema` caused a TypeError.
  Removed `dataset_name` from `open_params_schema` (was redundant due to 
  `data_id`) and added support for parameters `mosaicking_period`, 
  `upsampling`, `downsampling`, and `variable_fill_values`. (#94)

## Changes in 0.10.0

* Fixed Landsat radiance/reflectance bands to have 
  sample type `FLOAT32` by default, was `UNIT16`. (#89) 
* Avoid having `wavelength: NaN` values in band metadata attributes.
* Implemented performance optimizations for use of datasets 
  in xcube server. (#88)
* The open parameter `variable_names` in the `sentinelhub` data store  
  is now optional. 
* Fixed some import statements to be compatible with Python 3.10.

## Changes in 0.9.5

* If requesting a non-geographic target projection, spatial reference
  information is now provided in a CF-compliant way. (#85)

* Now logging when a new Sentinel Hub access token is fetched. 
  Also logging non-recoverable errors when accessing data.

## Changes in 0.9.4

* Avoiding `TokenExpiredError` that occurred after 
  requesting tiles from Sentinel Hub after more than 1h. (#49)


## Changes in 0.9.3

* Added header `SH-Tag: xcube-sh` to SH API requests. (#80)

* Sometimes a `requests.exceptions.ChunkedEncodingError` was raised
  when a large number of data cube tiles were requested concurrently.
  The error occurred in cases where the Sentinel Hub server was no longer
  able to stream response data. The problem has been mitigated by  
  applying the same retry strategy as for "normal" HTTP error codes.

* Changed spelling `SENTINEL Hub` into `Sentinel Hub` in docs and logs.

## Changes in 0.9.2

* It is now possible accessing BYOC/BYOD and DEM datasets without
  providing the `time_period` cube configuration parameter. 
  In case such a dataset has no associated time information,  
  we assume a single time step (size of dimension `time` is one)
  and assign the query time range to this time step. (#75, #67, #35)

* The cube configuration parameter `band_names` can now be omitted
  also for BYOC collections. In this case, the returned data cube 
  will contain all available using their native sample types. (#76)

* Introduced a new cube configuration parameter `band_fill_values` 
  that can be used to specify the fill value (= no-data value)
  for either all bands if given as a scalar numbers or for individual bands,
  if given as list or tuple of numbers. (#34)


## Changes in 0.9.1

* Added resampling parameters to cube configuration (#66) and
  made `"NEAREST"` the default for `upsampling` and `downsampling` 
  (was `"BILINEAR"`):
  ```python
  upsampling: str = "NEAREST"           # or "BILINEAR", "BICUBIC"
  downsampling: str = "NEAREST"         # or "BILINEAR", "BICUBIC"
  mosaicking_order: str = "mostRecent"  # or "leastRecent", "leastCC"
  ```
* Warnings saying 
  `RuntimeWarning: Failed to open Zarr store with consolidated metadata...`
  are now silenced. (#69)
* The xcube `sentinelhub` data store now correctly retrieves available
  dataset time ranges from Sentinel Hub catalog (#70)

## Changes in 0.9.0

* Version 0.9 now requires xcube 0.9 because of incompatible API changes 
  in the xcube data store framework. However, most user code should not  
  be affected.
* If the requested CRS is not geographic, the returned dataset will 
  now contain a variable named `crs` whose attributes encode the 
  dataset's spatial CRS in a CF-compliant way. (#64)
* Providing a `collection_id` and omitting `time_period` in `CubeConfig`
  raised a confusing exception. We now provide a better problem
  description. (#35)
* Added two new constructor parameters to `SentinelHub` class that override
  default SH API URLs derived from endpoint URL *api_url*:
  - `catalog_url`: Overrides default SH process API URL.
  - `process_url`: Overrides default SH catalog API URL.

## Changes in 0.8.1

* Fixed coordinate transformation into geographic CRS e.g. EPSG 4326. 
  In this case (lon, lat) pairs were expected, but (lat, lon) pairs 
  were received. (#60)
 
## Changes in 0.8.0

* Now supporting Landsat-8 Level-1 (`"LOTL1"`) and Level-2 (`"LOTL2"`) collections (#53, thanks @maximlamare for PR #54)
* Now works for `bbox` coordinates using a CRS other than CRS84, WGS84, EPSG:4326. (#55)
* Provided xcube data store framework interface compatibility with 
  breaking changes in xcube 0.8.0 (see https://github.com/dcs4cop/xcube/issues/420).

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
   
* Now using Sentinel Hub's Catalogue API instead of the WFS (#14)
  - to determine actual observations within given bounding box and time range;
  - to determine time range of available data collections.  

* The keyword argument `instance_id` of the `SentinelHub` and `SentinelHubStore` constructors
  has been deprecated. It is no longer required    

## Changes in 0.3.0.dev1
 
*Note, this version has been accidentally released as v0.5.0.*

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
