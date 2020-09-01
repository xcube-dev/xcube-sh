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

from typing import Iterator, Tuple, Optional

import xarray as xr
import zarr

from xcube.core.store import DataDescriptor
from xcube.core.store import DataOpener
from xcube.core.store import DataStore
from xcube.core.store import DataStoreError
from xcube.core.store import DatasetDescriptor
from xcube.core.store import TYPE_ID_DATASET
from xcube.core.store import VariableDescriptor
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
    """
    Sentinel HUB implementation of the ``xcube.core.store.DataOpener`` interface.

    Please refer to the :math:open_data method for the list of possible open parameters.
    """

    #############################################################################
    # Specific interface

    def __init__(self, sentinel_hub: SentinelHub = None):
        self._sentinel_hub = sentinel_hub

    def describe_data(self, data_id: str) -> DatasetDescriptor:
        dsd = self._describe_data(data_id)
        dsd.open_params_schema = self._get_open_data_params_schema(dsd)
        return dsd

    #############################################################################
    # DataOpener impl.

    def get_open_data_params_schema(self, data_id: str = None) -> JsonObjectSchema:
        return self._get_open_data_params_schema(self._describe_data(data_id))

    def open_data(self, data_id: str, **open_params) -> xr.Dataset:
        """
        Opens the dataset with given *data_id* and *open_params*.

        Possible values for *data_id* can be retrieved from the :meth:SentinelHubDataStore::get_data_ids method.
        Possible keyword-arguments in *open_params* are:

        * ``variable_names: Sequence[str]`` - optional list of variable names.
            If not given, all variables are included.
        * ``variable_units: Union[str, Sequence[str]]`` - units for all or each variable
        * ``variable_sample_types: Union[str, Sequence[str]]`` - sample types for all or each variable
        * ``crs: str`` - spatial CRS identifier. must be a valid OGC CRS URI.
        * ``tile_size: Tuple[int, int]`` - optional tuple of spatial tile sizes in pixels.
        * ``bbox: Tuple[float, float, float, float]`` - spatial coverage given as (minx, miny, maxx, maxy)
            in units of the CRS. Required parameter.
        * ``spatial_res: float`` - spatial resolution in unsits of the CRS^.
            Required parameter.
        * ``time_range: Tuple[Optional[str], Optional[str]]`` - tuple (start-time, end-time).
            Required parameter.
        * ``time_period: str`` - Pandas-compatible time period/frequency, e.g. "4D", "2W"
        * ``time_tolerance: str`` - Maximum time tolerance. Pandas-compatible time period/frequency.
        * ``collection_id: str`` - An identifier used by Sentinel HUB to identify BYOC datasets.
        * ``four_d: bool`` - If True, variables will represented as fourth dimension.

        In addition, all store parameters can be used, if the data opener is used on its own.
        See :meth:SentinelHubDataStore::get_data_store_params_schema method.

        :param data_id: The data identifier.
        :param open_params: Open parameters.
        :return: An xarray.Dataset instance
        """
        schema = self.get_open_data_params_schema(data_id)
        schema.validate_instance(open_params)

        sentinel_hub = self._sentinel_hub
        if sentinel_hub is None:
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
            sentinel_hub = SentinelHub(**sh_kwargs)

        cube_config_kwargs, open_params = schema.process_kwargs_subset(open_params, (
            'variable_names',
            'variable_units',
            'variable_sample_types',
            'crs',
            'tile_size',
            'bbox',
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

        band_names = cube_config_kwargs.pop('variable_names', None)
        band_units = cube_config_kwargs.pop('variable_units', None)
        band_sample_types = cube_config_kwargs.pop('variable_sample_types', None)
        cube_config = CubeConfig(dataset_name=data_id,
                                 band_names=band_names,
                                 band_units=band_units,
                                 band_sample_types=band_sample_types,
                                 **cube_config_kwargs)
        chunk_store = SentinelHubChunkStore(sentinel_hub, cube_config, **chunk_store_kwargs)
        max_cache_size = open_params.pop('max_cache_size', None)
        if max_cache_size:
            chunk_store = zarr.LRUStoreCache(chunk_store, max_size=max_cache_size)
        return xr.open_zarr(chunk_store, **open_params)

    #############################################################################
    # Implementation helpers

    def _get_open_data_params_schema(self, dsd: DatasetDescriptor = None) -> JsonObjectSchema:
        cube_params = dict(
            dataset_name=JsonStringSchema(min_length=1),
            variable_names=JsonArraySchema(
                items=JsonStringSchema(enum=[v.name for v in dsd.data_vars] if dsd and dsd.data_vars else None)),
            variable_units=JsonArraySchema(),
            variable_sample_types=JsonArraySchema(),
            tile_size=JsonArraySchema(items=(JsonNumberSchema(minimum=1, maximum=2500, default=DEFAULT_TILE_SIZE),
                                             JsonNumberSchema(minimum=1, maximum=2500, default=DEFAULT_TILE_SIZE)),
                                      default=(DEFAULT_TILE_SIZE, DEFAULT_TILE_SIZE)),
            crs=JsonStringSchema(default=DEFAULT_CRS),
            bbox=JsonArraySchema(items=(JsonNumberSchema(),
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
            'bbox',
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

    def _describe_data(self, data_id: str) -> DatasetDescriptor:
        md = SentinelHubMetadata()

        dataset_attrs = md.dataset(data_id)

        band_names = None
        if self._sentinel_hub is not None:
            dataset_item = next((item for item in self._sentinel_hub.datasets
                                 if item.get('id') == data_id), None)
            if dataset_item is None:
                raise DataStoreError(f'Unknown dataset identifier "{data_id}"')
            band_names = self._sentinel_hub.band_names(data_id)
            dataset_attrs = dict(**(dataset_attrs or {}))
            dataset_attrs['title'] = dataset_item.get('name')
        else:
            if dataset_attrs is None:
                raise DataStoreError(f'Unknown dataset identifier "{data_id}"')
            dataset_attrs = dict(**dataset_attrs)
            dataset_attrs.pop('bands', None)

        if not band_names:
            band_names = md.dataset_band_names(data_id) or []

        data_vars = [VariableDescriptor(name=band_name,
                                        dtype=md.dataset_band_sample_type(data_id, band_name),
                                        dims=('time', 'lat', 'lon'),
                                        attrs=md.dataset_band(data_id, band_name))
                     for band_name in band_names]

        return DatasetDescriptor(data_id=data_id,
                                 data_vars=data_vars,
                                 attrs=dataset_attrs)


class SentinelHubDataStore(SentinelHubDataOpener, DataStore):
    """
    Sentinel HUB implementation of the ``xcube.core.store.DataStore`` interface.
    """

    def __init__(self, **sh_kwargs):
        super().__init__(SentinelHub(**sh_kwargs))

    #############################################################################
    # DataStore impl.

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        sh_params = dict(
            client_id=JsonStringSchema(title='SENTINEL Hub API client identifier',
                                       description='Preferably set by environment variable SH_CLIENT_ID'),
            client_secret=JsonStringSchema(title='SENTINEL Hub API client secret',
                                           description='Preferably set by environment variable SH_CLIENT_SECRET'),
            instance_id=JsonStringSchema(nullable=True,
                                         title='SENTINEL Hub API instance identifier',
                                         description='Preferably set by environment variable SH_INSTANCE_ID'),
            api_url=JsonStringSchema(default=DEFAULT_SH_API_URL,
                                     title='SENTINEL Hub API URL'),
            oauth2_url=JsonStringSchema(default=DEFAULT_SH_OAUTH2_URL,
                                        title='SENTINEL Hub API authorisation URL'),
            enable_warnings=JsonBooleanSchema(default=False,
                                              title='Whether to output warnings'),
            error_policy=JsonStringSchema(default='fail', enum=['fail', 'warn', 'ignore'],
                                          title='Policy for errors while requesting data'),
            num_retries=JsonIntegerSchema(default=DEFAULT_NUM_RETRIES, minimum=0,
                                          title='Number of retries when requesting data fails'),
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

    def get_data_ids(self, type_id: str = None) -> Iterator[Tuple[str, Optional[str]]]:
        self._assert_valid_type_id(type_id)
        metadata = SentinelHubMetadata()
        for data_id, dataset in metadata.datasets.items():
            yield data_id, dataset.get('title')

    def has_data(self, data_id: str) -> bool:
        return data_id in SentinelHubMetadata().dataset_names

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

    #############################################################################
    # Implementation helpers

    def _assert_valid_type_id(self, type_id):
        if type_id is not None and type_id != TYPE_ID_DATASET:
            raise DataStoreError(f'Data type identifier must be "{TYPE_ID_DATASET}", but got "{type_id}"')

    def _assert_valid_opener_id(self, opener_id):
        if opener_id is not None and opener_id != SH_DATA_OPENER_ID:
            raise DataStoreError(f'Data opener identifier must be "{SH_DATA_OPENER_ID}", but got "{opener_id}"')
