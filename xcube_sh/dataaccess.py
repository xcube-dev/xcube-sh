# The MIT License (MIT)
# Copyright (c) 2020 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
from typing import Iterator

import xarray as xr
import zarr

from xcube.core.store.dataaccess import DataAccessor
from xcube.core.store.dataaccess import DatasetDescriber
from xcube.core.store.dataaccess import DatasetIterator
from xcube.core.store.dataaccess import ZarrDatasetOpener
from xcube.core.store.descriptor import DatasetDescriptor
from xcube.core.store.descriptor import VariableDescriptor
from xcube.util.jsonschema import JsonArraySchema
from xcube.util.jsonschema import JsonBooleanSchema
from xcube.util.jsonschema import JsonIntegerSchema
from xcube.util.jsonschema import JsonNumberSchema
from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import JsonStringSchema
from xcube_sh.config import CubeConfig
from xcube_sh.constants import DEFAULT_CLIENT_ID
from xcube_sh.constants import DEFAULT_CLIENT_SECRET
from xcube_sh.constants import DEFAULT_CRS
from xcube_sh.constants import DEFAULT_INSTANCE_ID
from xcube_sh.constants import DEFAULT_NUM_RETRIES
from xcube_sh.constants import DEFAULT_RETRY_BACKOFF_BASE
from xcube_sh.constants import DEFAULT_RETRY_BACKOFF_MAX
from xcube_sh.constants import DEFAULT_SH_API_URL
from xcube_sh.constants import DEFAULT_SH_OAUTH2_URL
from xcube_sh.constants import DEFAULT_TILE_SIZE
from xcube_sh.constants import DEFAULT_TIME_TOLERANCE
from xcube_sh.metadata import SentinelHubMetadata
from xcube_sh.sentinelhub import SentinelHub
from xcube_sh.store import SentinelHubChunkStore


class ZarrSentinelHubDatasetAccessor(ZarrDatasetOpener, DatasetDescriber, DatasetIterator, DataAccessor):

    def iter_dataset_ids(self) -> Iterator[str]:
        return iter(SentinelHubMetadata().dataset_names)

    def describe_dataset(self, dataset_id: str) -> DatasetDescriptor:
        # TODO
        md = SentinelHubMetadata()
        return DatasetDescriptor(dataset_id=dataset_id,
                                 data_vars=[VariableDescriptor(name=band_name,
                                                               dtype='FLOAT32',
                                                               dims=('time', 'lat', 'lon'),
                                                               attrs=md.dataset_band(dataset_id, band_name))
                                            for band_name in md.dataset_band_names(dataset_id)])

    def get_open_dataset_params_schema(self, dataset_id: str = None) -> JsonObjectSchema:
        dsd = self.describe_dataset(dataset_id) if dataset_id else None

        sh_params = dict(
            client_id=JsonStringSchema(default=DEFAULT_CLIENT_ID),
            client_secret=JsonStringSchema(default=DEFAULT_CLIENT_SECRET),
            instance_id=JsonStringSchema(default=DEFAULT_INSTANCE_ID, nullable=True),
            api_url=JsonStringSchema(default=DEFAULT_SH_API_URL),
            oauth2_url=JsonStringSchema(default=DEFAULT_SH_OAUTH2_URL),
            enable_warnings=JsonBooleanSchema(default=False),
            error_policy=JsonStringSchema(default='fail', enum=['fail', 'warn', 'ignore']),
            num_retries=JsonIntegerSchema(default=DEFAULT_NUM_RETRIES, minimum=0),
            retry_backoff_max=JsonIntegerSchema(default=DEFAULT_RETRY_BACKOFF_MAX, minimum=0),
            retry_backoff_base=JsonNumberSchema(default=DEFAULT_RETRY_BACKOFF_BASE, exclusive_minimum=1.0),
        )
        cube_params = dict(
            dataset_name=JsonStringSchema(min_length=1),
            band_names=JsonArraySchema(
                items=JsonStringSchema(enum=[v.name for v in dsd.data_vars] if dsd and dsd.data_vars else None)),
            band_units=JsonArraySchema(),
            band_sample_types=JsonArraySchema(),
            tile_size=JsonArraySchema(items=(JsonNumberSchema(minimum=1, maximum=2500, default=DEFAULT_TILE_SIZE),
                                             JsonNumberSchema(minimum=1, maximum=2500, default=DEFAULT_TILE_SIZE)),
                                      default=(DEFAULT_TILE_SIZE, DEFAULT_TILE_SIZE)),
            crs=JsonStringSchema(default=DEFAULT_CRS),
            geometry=JsonArraySchema(items=(JsonNumberSchema(),
                                            JsonNumberSchema(),
                                            JsonNumberSchema(),
                                            JsonNumberSchema())),
            spatial_res=JsonNumberSchema(exclusive_minimum=0.0),
            time_range=JsonArraySchema(items=(JsonStringSchema(format='date-time'),
                                              JsonStringSchema(format='date-time'))),
            # TODO: add pattern
            time_period=JsonStringSchema(default='1D'),
            time_tolerance=JsonStringSchema(default=DEFAULT_TIME_TOLERANCE),
            collection_id=JsonStringSchema(),
            four_d=JsonBooleanSchema(default=False),
        )
        cache_params = dict(
            max_cache_size=JsonIntegerSchema(),
        )
        # required cube_params
        required = [
            'band_names',
            'geometry',
            'spatial_res',
            'time_range',
        ]
        # required sh_params
        if DEFAULT_CLIENT_ID is None:
            required.append('client_id')
        if DEFAULT_CLIENT_SECRET is None:
            required.append('client_secret')
        return JsonObjectSchema(
            properties=dict(
                **sh_params,
                **cube_params,
                **cache_params
            ),
            required=required,
            additional_properties=False
        )

    def open_dataset(self, dataset_id: str, **open_params) -> xr.Dataset:
        schema = self.get_open_dataset_params_schema(dataset_id)
        schema.validate_instance(open_params)

        sh_kwargs, open_params = schema.process_kwargs_subset(open_params, (
            'client_id',
            'client_secret',
            'instance_id',
            'api_url',
            'oauth2_url',
            'enable_warnings',
            'error_policy',
            'num_retries',
            'retry_backoff_max',
            'retry_backoff_base',
        ))

        cube_config_kwargs, open_params = schema.process_kwargs_subset(open_params, (
            'band_names',
            'band_units',
            'band_sample_types',
            'tile_size',
            'geometry',
            'spatial_res',
            'time_range',
            'time_period',
            'time_tolerance',
            'collection_id',
            'four_d',
        ))

        chunk_store_kwargs, open_params = schema.process_kwargs_subset(open_params, (
            'observer',
            'trace_store_calls'
        ))

        sentinel_hub = SentinelHub(**sh_kwargs)
        cube_config = CubeConfig(dataset_name=dataset_id, **cube_config_kwargs)
        chunk_store = SentinelHubChunkStore(sentinel_hub, cube_config, **chunk_store_kwargs)
        max_cache_size = open_params.pop('max_cache_size', None)
        if max_cache_size:
            chunk_store = zarr.LRUStoreCache(chunk_store, max_size=max_cache_size)
        return xr.open_zarr(chunk_store, **open_params)
