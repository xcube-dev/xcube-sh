## Changes in 0.3 (in dev)

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
