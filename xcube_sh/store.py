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
from typing import Iterator, Tuple

import xarray as xr
import zarr

from xcube.core.store.accessor import DataOpener
from xcube.core.store.descriptor import DataDescriptor
from xcube.core.store.descriptor import DatasetDescriptor
from xcube.core.store.descriptor import TYPE_ID_DATASET
from xcube.core.store.descriptor import VariableDescriptor
from xcube.core.store.store import DataStore
from xcube.core.store.store import DataStoreError
from xcube.util.jsonschema import JsonArraySchema
from xcube.util.jsonschema import JsonBooleanSchema
from xcube.util.jsonschema import JsonIntegerSchema
from xcube.util.jsonschema import JsonNumberSchema
from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import JsonStringSchema
from xcube_sh.chunkstore import SentinelHubChunkStore
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
from xcube_sh.constants import SH_DATA_OPENER_ID
from xcube_sh.metadata import SentinelHubMetadata
from xcube_sh.sentinelhub import SentinelHub


class SentinelHubDataOpener(DataOpener):

    def __init__(self, sentinel_hub: SentinelHub = None):
        self._sentinel_hub = sentinel_hub

    def describe_data(self, data_id: str) -> DataDescriptor:
        if self._sentinel_hub is not None:
            # TODO: use self._sentinel_hub to make SH catalogue API calls
            pass
        md = SentinelHubMetadata()
        return DatasetDescriptor(data_id=data_id,
                                 data_vars=[VariableDescriptor(name=band_name,
                                                               dtype='FLOAT32',
                                                               dims=('time', 'lat', 'lon'),
                                                               attrs=md.dataset_band(data_id, band_name))
                                            for band_name in md.dataset_band_names(data_id)])

    def get_open_data_params_schema(self, data_id: str = None) -> JsonObjectSchema:
        dsd = self.describe_data(data_id) if data_id else None
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
            # TODO: rename into bbox
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
            max_cache_size=JsonIntegerSchema(minimum=0),
        )
        # required cube_params
        required = [
            'band_names',
            # TODO: rename into bbox
            'geometry',
            'spatial_res',
            'time_range',
        ]
        sh_params = {}
        if self._sentinel_hub is None:
            sh_schema = SentinelHubDataStore.get_data_store_params_schema()
            sh_params = sh_schema.properties
            required.extend(sh_schema.required or [])
        return JsonObjectSchema(
            properties=dict(
                **sh_params,
                **cube_params,
                **cache_params
            ),
            required=required
        )

    def open_data(self, data_id: str, **open_params) -> xr.Dataset:
        schema = self.get_open_data_params_schema(data_id)
        schema.validate_instance(open_params)

        sentinel_hub = self._sentinel_hub
        if sentinel_hub is None:
            sh_kwargs, open_params = schema.process_kwargs_subset(open_params, (
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
            sentinel_hub = SentinelHub(**sh_kwargs)

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

        cube_config = CubeConfig(dataset_name=data_id, **cube_config_kwargs)
        chunk_store = SentinelHubChunkStore(sentinel_hub, cube_config, **chunk_store_kwargs)
        max_cache_size = open_params.pop('max_cache_size', None)
        if max_cache_size:
            chunk_store = zarr.LRUStoreCache(chunk_store, max_size=max_cache_size)
        return xr.open_zarr(chunk_store, **open_params)


class SentinelHubDataStore(SentinelHubDataOpener, DataStore):

    def __init__(self, **sh_kwargs):
        super().__init__(SentinelHub(**sh_kwargs))

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
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
        required = None
        if not DEFAULT_CLIENT_ID or not DEFAULT_CLIENT_SECRET:
            required = []
            if DEFAULT_CLIENT_ID is None:
                required.append('client_id')
            if DEFAULT_CLIENT_SECRET is None:
                required.append('client_secret')
        return JsonObjectSchema(
            properties=sh_params,
            required=required,
            additional_properties=False
        )

    @classmethod
    def get_type_ids(cls) -> Tuple[str, ...]:
        return TYPE_ID_DATASET,

    def get_data_ids(self, type_id: str = None) -> Iterator[str]:
        self._assert_valid_type_id(type_id)
        return iter(SentinelHubMetadata().dataset_names)

    def describe_data(self, data_id: str) -> DataDescriptor:
        return super().describe_data(data_id)

    # noinspection PyTypeChecker
    def search_data(self, type_id: str = None, **search_params) -> Iterator[DataDescriptor]:
        self._assert_valid_type_id(type_id)
        # TODO: implement using new SENTINEL Hub catalogue API
        raise NotImplementedError()

    def get_data_opener_ids(self, data_id: str = None, type_id: str = None) -> Tuple[str, ...]:
        self._assert_valid_type_id(type_id)
        return SH_DATA_OPENER_ID,

    def get_open_data_params_schema(self, data_id: str = None, opener_id: str = None) -> JsonObjectSchema:
        self._assert_valid_opener_id(opener_id)
        return super().get_open_data_params_schema(data_id)

    def open_data(self, data_id: str, opener_id: str = None, **open_params) -> xr.Dataset:
        self._assert_valid_opener_id(opener_id)
        return super().open_data(data_id, **open_params)

    def _assert_valid_type_id(self, type_id):
        if type_id is not None and type_id != TYPE_ID_DATASET:
            raise DataStoreError(f'Data type identifier must be "{TYPE_ID_DATASET}", but got "{type_id}"')

    def _assert_valid_opener_id(self, opener_id):
        if opener_id is not None and opener_id != SH_DATA_OPENER_ID:
            raise DataStoreError(f'Data opener identifier must be "{SH_DATA_OPENER_ID}", but got "{opener_id}"')
