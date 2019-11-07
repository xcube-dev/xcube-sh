## Changes in 0.2 (in development)

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

## Changes in 0.1
 
Initial version for ESA Demo 1.
