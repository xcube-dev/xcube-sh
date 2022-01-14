# The MIT License (MIT)
# Copyright (c) 2022 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from typing import Iterator, Tuple, Optional, Dict, Any, Union, Container

import xarray as xr
import zarr
from xcube.core.store import DATASET_TYPE
from xcube.core.store import DataDescriptor
from xcube.core.store import DataOpener
from xcube.core.store import DataStore
from xcube.core.store import DataStoreError
from xcube.core.store import DataTypeLike
from xcube.core.store import DatasetDescriptor
from xcube.core.store import DefaultSearchMixin
from xcube.core.store import VariableDescriptor
from xcube.util.assertions import assert_not_none
from xcube.util.jsonschema import JsonArraySchema
from xcube.util.jsonschema import JsonBooleanSchema
from xcube.util.jsonschema import JsonDateSchema
from xcube.util.jsonschema import JsonIntegerSchema
from xcube.util.jsonschema import JsonNumberSchema
from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import JsonStringSchema

from .chunkstore import SentinelHubChunkStore
from .config import CubeConfig
from .constants import CRS_ID_TO_URI
from .constants import DEFAULT_CLIENT_ID
from .constants import DEFAULT_CLIENT_SECRET
from .constants import DEFAULT_CRS
from .constants import DEFAULT_MOSAICKING_ORDER
from .constants import DEFAULT_NUM_RETRIES
from .constants import DEFAULT_RESAMPLING
from .constants import DEFAULT_RETRY_BACKOFF_BASE
from .constants import DEFAULT_RETRY_BACKOFF_MAX
from .constants import DEFAULT_SH_API_URL
from .constants import DEFAULT_SH_OAUTH2_URL
from .constants import DEFAULT_TILE_SIZE
from .constants import DEFAULT_TIME_TOLERANCE
from .constants import MOSAICKING_ORDERS
from .constants import RESAMPLINGS
from .constants import SH_DATA_OPENER_ID
from .metadata import SentinelHubMetadata
from .sentinelhub import SentinelHub


class SentinelHubDataOpener(DataOpener):
    """
    Sentinel HUB implementation of the ``xcube.core.store.DataOpener``
    interface.

    Please refer to the :math:open_data method for the list of possible
    open parameters.
    """

    ##########################################################################
    # Specific interface

    def __init__(self, sentinel_hub: SentinelHub = None):
        self._sentinel_hub = sentinel_hub

    def describe_data(self, data_id: str, data_type: DataTypeLike = None) \
            -> DatasetDescriptor:
        # data_type is ignored, xcube-sh only provides "dataset"
        dsd = self._describe_data(data_id)
        dsd.open_params_schema = self._get_open_data_params_schema(dsd)
        return dsd

    ##########################################################################
    # DataOpener impl.

    def get_open_data_params_schema(self, data_id: str = None) \
            -> JsonObjectSchema:
        assert_not_none(data_id, 'data_id')
        return self._get_open_data_params_schema(self._describe_data(data_id))

    def open_data(self, data_id: str, **open_params) -> xr.Dataset:
        """
        Opens the dataset with given *data_id* and *open_params*.

        Possible values for *data_id* can be retrieved from the
        :meth:`SentinelHubDataStore`::`get_data_ids` method.
        Possible keyword-arguments in *open_params* are:

        * ``variable_names: Sequence[str]`` - optional list of variable names.
            If not given, all variables are included.
        * ``variable_units: Union[str, Sequence[str]]``
            - units for all or each variable
        * ``variable_sample_types: Union[str, Sequence[str]]``
            - sample types for all or each variable
        * ``crs: str`` - spatial CRS identifier. must be a valid OGC CRS URI.
        * ``tile_size: Tuple[int, int]``
            - optional tuple of spatial tile sizes in pixels.
        * ``bbox: Tuple[float, float, float, float]``
            - spatial coverage given as (minx, miny, maxx, maxy)
            in units of the CRS. Required parameter.
        * ``spatial_res: float``
            - spatial resolution in units of the CRS. Required parameter.
        * ``time_range: Tuple[Optional[str], Optional[str]]``
            - tuple (start-time, end-time).
            Both start-time and end-time, if given, should use ISO 8601
            format. Required parameter.
        * ``time_period: str``
            - Pandas-compatible time period/frequency, e.g. "4D", "2W"
        * ``time_tolerance: str``
            - Maximum time tolerance. Pandas-compatible time period/frequency.
        * ``collection_id: str``
            - An identifier used by Sentinel HUB to identify BYOC datasets.
        * ``four_d: bool``
            - If True, variables will represented as fourth dimension.

        In addition, all store parameters can be used, if the data
        opener is used on its own. See
        :meth:`SentinelHubDataStore`::`get_data_store_params_schema`
        method.

        :param data_id: The data identifier.
        :param open_params: Open parameters.
        :return: An xarray.Dataset instance
        """
        assert_not_none(data_id, 'data_id')

        schema = self.get_open_data_params_schema(data_id)
        schema.validate_instance(open_params)

        sentinel_hub = self._sentinel_hub
        if sentinel_hub is None:
            sh_kwargs, open_params = schema.process_kwargs_subset(
                open_params,
                (
                    'client_id',
                    'client_secret',
                    'api_url',
                    'oauth2_url',
                    'enable_warnings',
                    'error_policy',
                    'num_retries',
                    'retry_backoff_max',
                    'retry_backoff_base',
                )
            )
            sentinel_hub = SentinelHub(**sh_kwargs)

        cube_config_kwargs, open_params = schema.process_kwargs_subset(
            open_params,
            (
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
            )
        )

        chunk_store_kwargs, open_params = schema.process_kwargs_subset(
            open_params,
            (
                'observer',
                'trace_store_calls'
            )
        )

        band_names = cube_config_kwargs.pop('variable_names',
                                            None)
        band_sample_types = cube_config_kwargs.pop('variable_sample_types',
                                                   None)
        band_fill_values = cube_config_kwargs.pop('variable_fill_values',
                                                  None)
        band_units = cube_config_kwargs.pop('variable_units',
                                            None)

        cube_config = CubeConfig(dataset_name=data_id,
                                 band_names=band_names,
                                 band_units=band_units,
                                 band_sample_types=band_sample_types,
                                 band_fill_values=band_fill_values,
                                 **cube_config_kwargs)
        chunk_store = SentinelHubChunkStore(sentinel_hub, cube_config,
                                            **chunk_store_kwargs)
        max_cache_size = open_params.pop('max_cache_size', None)
        if max_cache_size:
            chunk_store = zarr.LRUStoreCache(chunk_store,
                                             max_size=max_cache_size)
        return xr.open_zarr(chunk_store, **open_params)

    ##########################################################################
    # Implementation helpers

    def _get_open_data_params_schema(self, dsd: DatasetDescriptor = None) \
            -> JsonObjectSchema:
        min_date, max_date = dsd.time_range \
            if dsd.time_range is not None else (None, None)

        cube_params = dict(
            dataset_name=JsonStringSchema(min_length=1),
            variable_names=JsonArraySchema(
                # Note, in xcube-sh < 0.9.2 formerly used an enum here.
                # However, that doesn't work for BYOC, so we omit that
                # limitation.
                items=JsonStringSchema()
            ),
            variable_fill_values=JsonArraySchema(
                items=JsonNumberSchema(nullable=True)
            ),
            variable_sample_types=JsonArraySchema(
                items=JsonStringSchema()
            ),
            variable_units=JsonArraySchema(
                items=JsonStringSchema()
            ),
            tile_size=JsonArraySchema(
                items=(
                    JsonNumberSchema(minimum=1,
                                     maximum=2500,
                                     default=DEFAULT_TILE_SIZE),
                    JsonNumberSchema(minimum=1,
                                     maximum=2500,
                                     default=DEFAULT_TILE_SIZE)
                ),
                default=(DEFAULT_TILE_SIZE, DEFAULT_TILE_SIZE)
            ),
            crs=JsonStringSchema(
                default=DEFAULT_CRS,
                enum=CRS_ID_TO_URI.keys()
            ),
            bbox=JsonArraySchema(
                items=(JsonNumberSchema(),
                       JsonNumberSchema(),
                       JsonNumberSchema(),
                       JsonNumberSchema())
            ),
            spatial_res=JsonNumberSchema(exclusive_minimum=0.0),
            upsampling=JsonStringSchema(
                default=DEFAULT_RESAMPLING,
                enum=RESAMPLINGS
            ),
            downsampling=JsonStringSchema(
                default=DEFAULT_RESAMPLING,
                enum=RESAMPLINGS
            ),
            mosaicking_order=JsonStringSchema(
                default=DEFAULT_MOSAICKING_ORDER,
                enum=MOSAICKING_ORDERS
            ),
            time_range=JsonDateSchema.new_range(
                min_date=min_date,
                max_date=max_date,
                nullable=True
            ),
            # TODO: add pattern
            time_period=JsonStringSchema(
                default='1D', nullable=True,
                enum=[None,
                      *map(lambda n: f'{n}D', range(1, 14)),
                      '1W', '2W']
            ),
            time_tolerance=JsonStringSchema(
                default=DEFAULT_TIME_TOLERANCE,
                format='^([1-9]*[0-9]*)[NULSTH]$'
            ),
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
            # If we are NOT connected to the API (yet), we also
            # include store parameters
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
        assert_not_none(data_id, 'data_id')
        dataset_metadata, collection_metadata = \
            self._get_dataset_and_collection_metadata(data_id)
        bands = dict(dataset_metadata.get('bands', {}))

        if self._sentinel_hub is not None and data_id.upper() != 'CUSTOM':
            # If we are connected to the API, we return band names by API
            remote_bands = self._sentinel_hub.bands(data_id)
            if remote_bands:
                for band in remote_bands:
                    band_copy = dict(band)
                    band_name = band_copy.pop('name')
                    if 'sampleType' in band_copy:
                        band_copy['sample_type'] = band_copy.pop('sampleType')
                    if band_name in bands:
                        bands[band_name].update(band_copy)
                    else:
                        bands[band_name] = band_copy

        data_vars = {}
        for band_name, band_attrs in bands.items():
            data_vars[band_name] = \
                VariableDescriptor(
                    name=band_name,
                    dtype=band_attrs.get('sample_type', 'FLOAT32'),
                    dims=('time', 'y', 'x'),
                    attrs=band_attrs.copy()
                )

        dataset_attrs = dataset_metadata.copy()

        bbox = None
        time_range = None
        if collection_metadata is not None:
            extent = collection_metadata.get('extent')
            if extent is not None:
                bbox = extent.get("spatial", {}).get('bbox')
                if isinstance(bbox, list) and len(bbox) > 0:
                    if isinstance(bbox[0], list) and len(bbox[0]) == 4:
                        bbox = tuple(bbox[0])
                    elif len(bbox) == 4:
                        bbox = tuple(bbox)
                interval = extent.get("temporal", {}).get('interval')
                if isinstance(interval, list) and len(interval) > 0:
                    if isinstance(interval[0], list) \
                            and len(interval[0]) == 2:
                        min_datetime, max_datetime = interval[0]
                    else:
                        min_datetime, max_datetime = interval
                    if min_datetime or max_datetime:
                        # Get rid of time part
                        time_range = (
                            min_datetime.split('T')[0]
                            if min_datetime is not None else None,
                            max_datetime.split('T')[0]
                            if max_datetime is not None else None
                        )

            if 'title' in collection_metadata:
                dataset_attrs['title'] = \
                    collection_metadata['title']
            if 'description' in collection_metadata:
                dataset_attrs['description'] = \
                    collection_metadata['description']

        return DatasetDescriptor(
            data_id=data_id,
            data_vars=data_vars,
            bbox=bbox,
            time_range=time_range,
            time_period=dataset_metadata.get('request_period'),
            attrs=dataset_metadata
        )

    def _get_dataset_and_collection_metadata(self, data_id: str) \
            -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        if data_id.upper() == 'CUSTOM':
            return {}, None
        dataset_metadata = SentinelHubMetadata().datasets.get(data_id)
        if dataset_metadata is None:
            dataset_metadata = {}
        if self._sentinel_hub is not None:
            # If we are connected to the API,
            # we may also have collection metadata
            collection_name = dataset_metadata.get('collection_name')
            if collection_name is not None:
                for collection_metadata in self._sentinel_hub.collections():
                    if collection_name == collection_metadata.get('id'):
                        return dataset_metadata, collection_metadata
        return dataset_metadata, None


class SentinelHubDataStore(DefaultSearchMixin,
                           SentinelHubDataOpener,
                           DataStore):
    """
    Sentinel HUB implementation of the ``xcube.core.store.DataStore``
    interface.
    """

    def __init__(self, **sh_kwargs):
        super().__init__(SentinelHub(**sh_kwargs))

    ##########################################################################
    # DataStore impl.

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        sh_params = dict(
            client_id=JsonStringSchema(
                title='Sentinel Hub API client identifier',
                description='Preferably set by environment'
                            ' variable SH_CLIENT_ID'
            ),
            client_secret=JsonStringSchema(
                title='Sentinel Hub API client secret',
                description='Preferably set by environment'
                            ' variable SH_CLIENT_SECRET'
            ),
            api_url=JsonStringSchema(
                default=DEFAULT_SH_API_URL,
                title='Sentinel Hub API URL'
            ),
            oauth2_url=JsonStringSchema(
                default=DEFAULT_SH_OAUTH2_URL,
                title='Sentinel Hub API authorisation URL'
            ),
            enable_warnings=JsonBooleanSchema(
                default=False,
                title='Whether to output warnings'
            ),
            error_policy=JsonStringSchema(
                default='fail',
                enum=['fail', 'warn', 'ignore'],
                title='Policy for errors while requesting data'
            ),
            num_retries=JsonIntegerSchema(
                default=DEFAULT_NUM_RETRIES,
                minimum=0,
                title='Number of retries when requesting data fails'
            ),
            retry_backoff_max=JsonIntegerSchema(
                default=DEFAULT_RETRY_BACKOFF_MAX, minimum=0
            ),
            retry_backoff_base=JsonNumberSchema(
                default=DEFAULT_RETRY_BACKOFF_BASE, exclusive_minimum=1.0
            ),
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
    def get_data_types(cls) -> Tuple[str, ...]:
        return DATASET_TYPE.alias,

    def get_data_types_for_data(self, data_id: str) -> Tuple[str, ...]:
        return self.get_data_types()

    def get_data_ids(self,
                     data_type: str = None,
                     include_attrs: Container[str] = None) -> \
            Union[Iterator[str], Iterator[Tuple[str, Dict[str, Any]]]]:
        return_tuples = include_attrs is not None
        # TODO: respect names other than "title" in include_attrs
        include_titles = return_tuples and 'title' in include_attrs
        if self._is_supported_data_type(data_type):
            if self._sentinel_hub is not None:
                metadata = SentinelHubMetadata()
                extra_collections = metadata.extra_collections(
                    self._sentinel_hub.api_url
                )
                # If we are connected to the API, we will return only
                # datasets that are also collections
                collections = self._sentinel_hub.collections()
                collections.extend(extra_collections)
                collection_datasets = metadata.collection_datasets
                for collection in collections:
                    collection_id = collection.get('id')
                    collection_title = collection.get('title') \
                        if include_titles else None
                    collection_dataset = collection_datasets.get(
                        collection_id
                    )
                    if collection_dataset is not None:
                        dataset_name = collection_dataset.get('dataset_name')
                        if dataset_name is not None:
                            if return_tuples:
                                if include_titles:
                                    yield dataset_name, {
                                        'title': collection_title
                                    }
                                else:
                                    yield dataset_name, {}
                            else:
                                yield dataset_name
            else:
                datasets = SentinelHubMetadata().datasets
                for dataset_name, dataset_metadata in datasets.items():
                    if return_tuples:
                        if include_titles:
                            yield dataset_name, {
                                'title': dataset_metadata.get('title')
                            }
                        else:
                            yield dataset_name, {}
                    else:
                        yield dataset_name

    def has_data(self,
                 data_id: str,
                 data_type: str = None) -> bool:
        if self._is_supported_data_type(data_type):
            return data_id in SentinelHubMetadata().dataset_names
        return False

    def describe_data(self, data_id: str, data_type: str = None) \
            -> DataDescriptor:
        # data_type_alias is ignored, xcube-sh only provides "dataset"
        return super().describe_data(data_id, data_type=data_type)

    def get_data_opener_ids(self,
                            data_id: str = None,
                            data_type: str = None) -> Tuple[str, ...]:
        if self._is_supported_data_type(data_type):
            return SH_DATA_OPENER_ID,
        return ()

    def get_open_data_params_schema(self,
                                    data_id: str = None,
                                    opener_id: str = None) \
            -> JsonObjectSchema:
        self._assert_valid_opener_id(opener_id)
        return super().get_open_data_params_schema(data_id)

    def open_data(self,
                  data_id: str,
                  opener_id: str = None,
                  **open_params) -> xr.Dataset:
        self._assert_valid_opener_id(opener_id)
        return super().open_data(data_id, **open_params)

    ##########################################################################
    # Implementation helpers

    @classmethod
    def _is_supported_data_type(cls, data_type: DataTypeLike):
        return data_type is None or DATASET_TYPE.is_super_type_of(data_type)

    @classmethod
    def _assert_valid_opener_id(cls, opener_id):
        if opener_id is not None and opener_id != SH_DATA_OPENER_ID:
            raise DataStoreError(f'Data opener identifier must be'
                                 f' "{SH_DATA_OPENER_ID}",'
                                 f' but got "{opener_id}"')
