import xarray as xr
import zarr

from xcube.core.store.dataaccess import ZarrDatasetOpener
from xcube.util.jsonschema import JsonArraySchema
from xcube.util.jsonschema import JsonBooleanSchema
from xcube.util.jsonschema import JsonIntegerSchema
from xcube.util.jsonschema import JsonNumberSchema
from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import JsonStringSchema
from xcube_cci.config import CubeConfig
from xcube_sh.constants import DEFAULT_NUM_RETRIES, DEFAULT_RETRY_BACKOFF_MAX, DEFAULT_RETRY_BACKOFF_BASE, \
    DEFAULT_SH_API_URL, DEFAULT_SH_OAUTH2_URL, DEFAULT_CRS, DEFAULT_TIME_TOLERANCE
from xcube_sh.sentinelhub import SentinelHub
from xcube_sh.store import SentinelHubStore



class ZarrSentinelHubDatasetOpener(ZarrDatasetOpener):
    @property
    def write_dataset_params_schema(self) -> JsonObjectSchema:
        # TODO: extract individual parameter schemas as constants
        sh_params = dict(
            client_id=JsonStringSchema(),
            client_secret=JsonStringSchema(),
            instance_id=JsonStringSchema(nullable=True),
            api_url=JsonStringSchema(default=DEFAULT_SH_API_URL),
            oauth2_url=JsonStringSchema(default=DEFAULT_SH_OAUTH2_URL),
            enable_warnings=JsonBooleanSchema(default=False),
            error_policy=JsonStringSchema(default='fail'),
            num_retries=JsonIntegerSchema(default=DEFAULT_NUM_RETRIES),
            retry_backoff_max=JsonIntegerSchema(default=DEFAULT_RETRY_BACKOFF_MAX),
            retry_backoff_base=JsonNumberSchema(default=DEFAULT_RETRY_BACKOFF_BASE),
        )
        cube_params = dict(
            dataset_name=JsonStringSchema(min_length=1),
            band_names=JsonArraySchema(),
            band_units=JsonArraySchema(),
            band_sample_types=JsonArraySchema(),
            tile_size=JsonArraySchema(items=[JsonNumberSchema(minimum=1, maximum=2500),
                                             JsonNumberSchema(minimum=1, maximum=2500)]),
            geometry=JsonArraySchema(items=[JsonNumberSchema(),
                                            JsonNumberSchema(),
                                            JsonNumberSchema(),
                                            JsonNumberSchema()]),
            spatial_res=JsonNumberSchema(exclusive_minimum=0.0),
            crs=JsonStringSchema(default=DEFAULT_CRS),
            time_range=JsonArraySchema(items=[JsonStringSchema(format='date-time'),
                                              JsonStringSchema(format='date-time')]),
            time_period=JsonStringSchema(default='1D'),
            time_tolerance=JsonStringSchema(default=DEFAULT_TIME_TOLERANCE),
            collection_id=JsonStringSchema(),
            four_d=JsonBooleanSchema(default=False),
        )
        cache_params = dict(
            max_cache_size=JsonIntegerSchema(),
        )
        return JsonObjectSchema(
            properties=dict(
                **sh_params,
                **cube_params,
                **cache_params
            ),
            required=[
                # sh_params
                'client_id',
                'client_secret',
                # cube_params
                'dataset_name',
                'geometry',
                'spatial_res',
                'time_range',
            ],
            additional_properties=False
        )

    def open_dataset(self, path: str, **open_params) -> xr.Dataset:
        # TODO: use function that pops from open_params using the extracted individual parameter schemas constants
        sh_kwargs = dict(
            client_id=open_params.pop('client_id'),
            client_secret=open_params.pop('client_secret'),
            instance_id=open_params.pop('instance_id', None),
            api_url=open_params.pop('api_url', DEFAULT_SH_API_URL),
            oauth2_url=open_params.pop('oauth2_url', DEFAULT_SH_OAUTH2_URL),
            enable_warnings=open_params.pop('enable_warnings', False),
            error_policy=open_params.pop('error_policy', 'fail'),
            num_retries=open_params.pop('num_retries', DEFAULT_NUM_RETRIES),
            retry_backoff_max=open_params.pop('retry_backoff_max', DEFAULT_RETRY_BACKOFF_MAX),
            retry_backoff_base=open_params.pop('retry_backoff_base', DEFAULT_RETRY_BACKOFF_BASE),
        )
        cube_config_kwargs = dict(
            dataset_name=open_params.pop('dataset_name'),
            band_names=open_params.pop('band_names', None),
            band_units=open_params.pop('band_units', None),
            band_sample_types=open_params.pop('band_sample_types', None),
            tile_size=open_params.pop('tile_size', None),
            geometry=open_params.pop('geometry'),
            spatial_res=open_params.pop('spatial_res'),
            crs=open_params.pop('crs', DEFAULT_CRS),
            time_range=open_params.pop('time_range'),
            time_period=open_params.pop('time_period', None),
            time_tolerance=open_params.pop('time_tolerance', DEFAULT_TIME_TOLERANCE),
            collection_id=open_params.pop('collection_id', None),
            four_d=open_params.pop('four_d', False),
        )
        chunk_store_kwargs = dict(
            observer=open_params.pop('observer', None),
            trace_store_calls=open_params.pop('trace_store_calls', None)
        )
        sentinel_hub = SentinelHub(**sh_kwargs)
        cube_config = CubeConfig(**cube_config_kwargs)
        cube_store = SentinelHubStore(sentinel_hub, cube_config, **chunk_store_kwargs)
        max_cache_size = open_params.pop('max_cache_size', None)
        if max_cache_size:
            cube_store = zarr.LRUStoreCache(cube_store, max_cache_size)
        return xr.open_zarr(cube_store, **open_params)
