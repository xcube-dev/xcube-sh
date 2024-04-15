# Copyright © 2022-2024 by the xcube development team and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import itertools
import json
import math
import time
from abc import abstractmethod, ABCMeta
from collections.abc import MutableMapping
from typing import Iterator, Any, List, Dict, Tuple, Callable, Iterable, KeysView

import numpy as np
import pandas as pd
import pyproj
from numcodecs import Blosc

from .config import CubeConfig
from .constants import BAND_DATA_ARRAY_NAME
from .constants import CRS_ID_TO_URI
from .sentinelhub import SentinelHub
from .sentinelhub import SentinelHubError

_STATIC_ARRAY_COMPRESSOR_PARAMS = dict(
    cname="zstd", clevel=1, shuffle=Blosc.SHUFFLE, blocksize=0
)

_STATIC_ARRAY_COMPRESSOR_CONFIG = dict(id="blosc", **_STATIC_ARRAY_COMPRESSOR_PARAMS)

_STATIC_ARRAY_COMPRESSOR = Blosc(**_STATIC_ARRAY_COMPRESSOR_PARAMS)


def _dict_to_bytes(d: Dict) -> bytes:
    return _str_to_bytes(json.dumps(d, indent=2))


def _bytes_to_dict(b: bytes) -> Dict:
    return json.loads(_bytes_to_str(b))


def _str_to_bytes(s: str):
    return bytes(s, encoding="utf-8")


def _bytes_to_str(b: bytes) -> str:
    return b.decode("utf-8")


class RemoteStore(MutableMapping, metaclass=ABCMeta):
    """
    A remote Zarr Store.

    :param cube_config: Cube configuration.
    :param observer: An optional callback function called when remote
        requests are mode: observer(**kwargs).
    :param trace_store_calls: Whether store calls shall be
        printed (for debugging).
    """

    def __init__(
        self,
        cube_config: CubeConfig,
        observer: Callable = None,
        trace_store_calls=False,
    ):
        self._cube_config = cube_config
        self._observers = [observer] if observer is not None else []
        self._trace_store_calls = trace_store_calls
        self._time_ranges = self.get_time_ranges()

        if not self._time_ranges:
            raise ValueError("Could not determine any valid time stamps")

        width, height = self._cube_config.size
        spatial_res = self._cube_config.spatial_res
        x1, y1, x2, y2 = self._cube_config.bbox
        x_array = np.linspace(
            x1 + spatial_res / 2, x2 - spatial_res / 2, width, dtype=np.float64
        )
        y_array = np.linspace(
            y2 - spatial_res / 2, y1 + spatial_res / 2, height, dtype=np.float64
        )

        crs = pyproj.CRS.from_string(cube_config.crs)

        def time_stamp_to_str(ts: pd.Timestamp) -> str:
            """
            Convert to ISO string and strip timezone.
            Used to create numpy datetime64 arrays.
            We cannot create directly from pd.Timestamp because Numpy doesn't
            like parsing timezones anymore.
            """
            ts_str: str = ts.isoformat()
            if ts_str[-1] == "Z":
                return ts_str[0:-1]
            try:
                i = ts_str.rindex("+")
                return ts_str[0:i]
            except ValueError:
                return ts_str

        t_array = np.array(
            [time_stamp_to_str(s + 0.5 * (e - s)) for s, e in self._time_ranges],
            dtype="datetime64[s]",
        ).astype(np.int64)
        t_bnds_array = np.array(
            [
                [time_stamp_to_str(s), time_stamp_to_str(e)]
                for s, e in self._time_ranges
            ],
            dtype="datetime64[s]",
        ).astype(np.int64)

        time_coverage_start = self._time_ranges[0][0]
        time_coverage_end = self._time_ranges[-1][1]
        global_attrs = dict(
            Conventions="CF-1.7",
            coordinates="time_bnds",
            title=f"{self._cube_config.dataset_name} Data Cube Subset",
            history=[
                dict(
                    program=f"{self._class_name}",
                    cube_config=self._cube_config.to_dict(),
                )
            ],
            date_created=pd.Timestamp.now().isoformat(),
            time_coverage_start=time_coverage_start.isoformat(),
            time_coverage_end=time_coverage_end.isoformat(),
            time_coverage_duration=(
                time_coverage_end - time_coverage_start
            ).isoformat(),
        )

        if self._cube_config.time_period:
            time_period = self._cube_config.time_period.isoformat()
            global_attrs.update(time_coverage_resolution=time_period)

        if crs.is_geographic:
            x1, y2, x2, y2 = self._cube_config.bbox
            global_attrs.update(
                geospatial_lon_min=x1,
                geospatial_lat_min=y1,
                geospatial_lon_max=x2,
                geospatial_lat_max=y2,
            )

        processing_level = SentinelHub.METADATA.dataset_processing_level(
            self._cube_config.dataset_name
        )
        if processing_level:
            global_attrs.update(processing_level=processing_level)

        # setup Virtual File System (vfs)
        self._vfs = {
            ".zgroup": _dict_to_bytes(dict(zarr_format=2)),
            ".zattrs": _dict_to_bytes(global_attrs),
        }

        if crs.is_geographic:
            x_name, y_name = "lon", "lat"
            x_attrs, y_attrs = (
                {
                    "_ARRAY_DIMENSIONS": ["lon"],
                    "units": "decimal_degrees",
                    "long_name": "longitude",
                    "standard_name": "longitude",
                },
                {
                    "_ARRAY_DIMENSIONS": ["lat"],
                    "units": "decimal_degrees",
                    "long_name": "latitude",
                    "standard_name": "latitude",
                },
            )
        else:
            x_name, y_name = "x", "y"
            x_attrs, y_attrs = (
                {
                    "_ARRAY_DIMENSIONS": ["x"],
                    "long_name": "x coordinate of projection",
                    "standard_name": "projection_x_coordinate",
                },
                {
                    "_ARRAY_DIMENSIONS": ["y"],
                    "long_name": "y coordinate of projection",
                    "standard_name": "projection_y_coordinate",
                },
            )

        time_attrs = {
            "_ARRAY_DIMENSIONS": ["time"],
            "units": "seconds since 1970-01-01T00:00:00Z",
            "calendar": "proleptic_gregorian",
            "standard_name": "time",
            "bounds": "time_bnds",
        }
        time_bnds_attrs = {
            "_ARRAY_DIMENSIONS": ["time", "bnds"],
            "units": "seconds since 1970-01-01T00:00:00Z",
            "calendar": "proleptic_gregorian",
            "standard_name": "time",
        }

        self._add_static_array(x_name, x_array, x_attrs)
        self._add_static_array(y_name, y_array, y_attrs)
        self._add_static_array("time", t_array, time_attrs)
        self._add_static_array("time_bnds", t_bnds_array, time_bnds_attrs)

        crs_var_attrs = dict()
        if not crs.is_geographic:
            self._add_static_array(
                "crs",
                np.array(0),
                dict(
                    _ARRAY_DIMENSIONS=[],
                    **crs.to_cf(),
                ),
            )
            crs_var_attrs["grid_mapping"] = "crs"

        if self._cube_config.four_d:
            if crs.is_geographic:
                band_array_dimensions = ["time", "lat", "lon", "band"]
            else:
                band_array_dimensions = ["time", "y", "x", "band"]
            tile_width, tile_height = self._cube_config.tile_size
            num_bands = len(self._cube_config.band_names)
            self._add_static_array(
                "band",
                np.array(self._cube_config.band_names),
                attrs=dict(_ARRAY_DIMENSIONS=["band"]),
            )
            band_encoding = self.get_band_encoding(BAND_DATA_ARRAY_NAME)
            band_attrs = self.get_band_attrs(BAND_DATA_ARRAY_NAME)
            band_attrs.update(
                _ARRAY_DIMENSIONS=band_array_dimensions,
                band_names=self._cube_config.band_names,
            )
            self._add_remote_array(
                BAND_DATA_ARRAY_NAME,
                [t_array.size, height, width, num_bands],
                [1, tile_height, tile_width, num_bands],
                band_encoding,
                {**band_attrs, **crs_var_attrs},
            )
        else:
            if crs.is_geographic:
                band_array_dimensions = ["time", "lat", "lon"]
            else:
                band_array_dimensions = ["time", "y", "x"]
            tile_width, tile_height = self._cube_config.tile_size
            for band_name in self._cube_config.band_names:
                band_encoding = self.get_band_encoding(band_name)
                band_attrs = self.get_band_attrs(band_name)
                band_attrs.update(_ARRAY_DIMENSIONS=band_array_dimensions)
                self._add_remote_array(
                    band_name,
                    [t_array.size, height, width],
                    [1, tile_height, tile_width],
                    band_encoding,
                    {**band_attrs, **crs_var_attrs},
                )

        self._consolidate_metadata()

    def get_time_ranges(self) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        time_start, time_end = self._cube_config.time_range
        time_period = self._cube_config.time_period
        time_ranges = []
        time_now = time_start
        while time_now <= time_end:
            time_next = time_now + time_period
            time_ranges.append((time_now, time_next))
            time_now = time_next
        return time_ranges

    def add_observer(self, observer: Callable):
        """
        Add a request observer.

        :param observer: A callback function called when remote
            requests are mode: observer(**kwargs).
        """
        self._observers.append(observer)

    @abstractmethod
    def get_band_encoding(self, band_name: str) -> Dict[str, Any]:
        """
        Get the encoding settings for band (variable) *band_name*.
        Must at least contain "dtype" whose value is a numpy
            array-protocol type string.
        Refer to
        https://docs.scipy.org/doc/numpy/reference/arrays.interface.html#arrays-interface
        and zarr format 2 spec.
        """

    @abstractmethod
    def get_band_attrs(self, band_name: str) -> Dict[str, Any]:
        """
        Get any metadata attributes for band (variable) *band_name*.
        """

    def request_bbox(
        self, x_tile_index: int, y_tile_index: int
    ) -> Tuple[float, float, float, float]:
        x_tile_size, y_tile_size = self.cube_config.tile_size

        x_index = x_tile_index * x_tile_size
        y_index = y_tile_index * y_tile_size

        x01, _, _, y02 = self.cube_config.bbox
        spatial_res = self.cube_config.spatial_res

        x1 = x01 + spatial_res * x_index
        x2 = x01 + spatial_res * (x_index + x_tile_size)
        y1 = y02 - spatial_res * (y_index + y_tile_size)
        y2 = y02 - spatial_res * y_index

        return x1, y1, x2, y2

    def request_time_range(self, time_index: int) -> Tuple[pd.Timestamp, pd.Timestamp]:
        start_time, end_time = self._time_ranges[time_index]
        if self.cube_config.time_tolerance:
            start_time -= self.cube_config.time_tolerance
            end_time += self.cube_config.time_tolerance
        return start_time, end_time

    def _add_static_array(self, name: str, array: np.ndarray, attrs: Dict):
        shape = list(map(int, array.shape))
        dtype = str(array.dtype.str)
        order = "C"
        array_metadata = {
            "zarr_format": 2,
            "chunks": shape,
            "shape": shape,
            "dtype": dtype,
            "fill_value": None,
            "compressor": _STATIC_ARRAY_COMPRESSOR_CONFIG,
            "filters": None,
            "order": order,
        }
        chunk_key = ".".join(["0"] * array.ndim)
        self._vfs[name] = _str_to_bytes("")
        self._vfs[name + "/.zarray"] = _dict_to_bytes(array_metadata)
        self._vfs[name + "/.zattrs"] = _dict_to_bytes(attrs)
        self._vfs[name + "/" + chunk_key] = _STATIC_ARRAY_COMPRESSOR.encode(
            array.tobytes(order=order)
        )

    def _add_remote_array(
        self,
        name: str,
        shape: List[int],
        chunks: List[int],
        encoding: Dict[str, Any],
        attrs: Dict,
    ):
        array_metadata = dict(
            zarr_format=2,
            shape=shape,
            chunks=chunks,
            compressor=None,
            fill_value=None,
            filters=None,
            order="C",
        )
        array_metadata.update(encoding)
        self._vfs[name] = _str_to_bytes("")
        self._vfs[name + "/.zarray"] = _dict_to_bytes(array_metadata)
        self._vfs[name + "/.zattrs"] = _dict_to_bytes(attrs)
        nums = np.array(shape) // np.array(chunks)
        indexes = itertools.product(*tuple(map(range, map(int, nums))))
        for index in indexes:
            filename = ".".join(map(str, index))
            # noinspection PyTypeChecker
            self._vfs[name + "/" + filename] = name, index

    @property
    def cube_config(self) -> CubeConfig:
        return self._cube_config

    def _fetch_chunk(
        self, key: str, band_name: str, chunk_index: Tuple[int, ...]
    ) -> bytes:
        if len(chunk_index) == 4:
            time_index, y_chunk_index, x_chunk_index, band_index = chunk_index
        else:
            time_index, y_chunk_index, x_chunk_index = chunk_index

        request_bbox = self.request_bbox(x_chunk_index, y_chunk_index)
        request_time_range = self.request_time_range(time_index)

        t0 = time.perf_counter()
        try:
            exception = None
            chunk_data = self.fetch_chunk(
                key,
                band_name,
                chunk_index,
                bbox=request_bbox,
                time_range=request_time_range,
            )
        except Exception as e:
            exception = e
            chunk_data = None
        duration = time.perf_counter() - t0

        for observer in self._observers:
            observer(
                band_name=band_name,
                chunk_index=chunk_index,
                bbox=request_bbox,
                time_range=request_time_range,
                duration=duration,
                exception=exception,
            )

        if exception:
            raise exception

        return chunk_data

    @abstractmethod
    def fetch_chunk(
        self,
        key: str,
        band_name: str,
        chunk_index: Tuple[int, ...],
        bbox: Tuple[float, float, float, float],
        time_range: Tuple[pd.Timestamp, pd.Timestamp],
    ) -> bytes:
        """
        Fetch chunk data from remote.

        :param key: The original chunk key being retrieved.
        :param band_name: Band name.
        :param chunk_index: 3D chunk index (time, y, x).
        :param bbox: Requested bounding box in coordinate units of the CRS.
        :param time_range: Requested time range.
        :return: chunk data as raw bytes.
        """
        pass

    def _consolidate_metadata(self):
        # Consolidate metadata to suppress warning:  (#69)
        #
        # RuntimeWarning: Failed to open Zarr store with consolidated
        # metadata, falling back to try reading non-consolidated
        # metadata. ...
        #
        metadata = dict()
        for k, v in self._vfs.items():
            if (
                k == ".zattrs"
                or k.endswith("/.zattrs")
                or k == ".zarray"
                or k.endswith("/.zarray")
                or k == ".zgroup"
                or k.endswith("/.zgroup")
            ):
                metadata[k] = _bytes_to_dict(v)
        self._vfs[".zmetadata"] = _dict_to_bytes(
            dict(zarr_consolidated_format=1, metadata=metadata)
        )

    @property
    def _class_name(self):
        return self.__module__ + "." + self.__class__.__name__

    ##########################################################################
    # Zarr Store (MutableMapping) implementation
    ##########################################################################

    def keys(self) -> KeysView[str]:
        if self._trace_store_calls:
            print(f"{self._class_name}.keys()")
        return self._vfs.keys()

    def listdir(self, key: str) -> Iterable[str]:
        if self._trace_store_calls:
            print(f"{self._class_name}.listdir(key={key!r})")
        if key == "":
            return list((k for k in self._vfs.keys() if "/" not in k))
        else:
            prefix = key + "/"
            start = len(prefix)
            return list(
                (
                    k
                    for k in self._vfs.keys()
                    if k.startswith(prefix) and k.find("/", start) == -1
                )
            )

    def getsize(self, key: str) -> int:
        if self._trace_store_calls:
            print(f"{self._class_name}.getsize(key={key!r})")
        return len(self._vfs[key])

    def __iter__(self) -> Iterator[str]:
        if self._trace_store_calls:
            print(f"{self._class_name}.__iter__()")
        return iter(self._vfs.keys())

    def __len__(self) -> int:
        if self._trace_store_calls:
            print(f"{self._class_name}.__len__()")
        return len(self._vfs.keys())

    def __contains__(self, key) -> bool:
        if self._trace_store_calls:
            print(f"{self._class_name}.__contains__(key={key!r})")
        return key in self._vfs

    def __getitem__(self, key: str) -> bytes:
        if self._trace_store_calls:
            print(f"{self._class_name}.__getitem__(key={key!r})")
        value = self._vfs[key]
        if isinstance(value, tuple):
            return self._fetch_chunk(key, *value)
        return value

    def __setitem__(self, key: str, value: bytes) -> None:
        if self._trace_store_calls:
            print(f"{self._class_name}.__setitem__(key={key!r}, value={value!r})")
        raise TypeError(f"{self._class_name} is read-only")

    def __delitem__(self, key: str) -> None:
        if self._trace_store_calls:
            print(f"{self._class_name}.__delitem__(key={key!r})")
        raise TypeError(f"{self._class_name} is read-only")


class SentinelHubChunkStore(RemoteStore):
    """
    A remote Zarr Store using SentinelHub as backend.

    :param sentinel_hub: SentinelHub instance.
    :param cube_config: Cube configuration.
    :param observer: An optional callback function called when
        remote requests are mode: observer(**kwargs).
    :param trace_store_calls: Whether store calls shall be
        printed (for debugging).
    """

    _SAMPLE_TYPE_TO_DTYPE = {
        # Note: Sentinel Hub currently only supports unsigned
        # integer values therefore requesting INT8 or INT16
        # will return the same as UINT8 or UINT16 respectively.
        "UINT8": "|u1",
        "UINT16": "<u2",
        "UINT32": "<u4",
        "INT8": "|u1",
        "INT16": "<u2",
        "INT32": "<u4",
        "FLOAT32": "<f4",
        "FLOAT64": "<f8",
    }

    _METADATA = SentinelHub.METADATA

    def __init__(
        self,
        sentinel_hub: SentinelHub,
        cube_config: CubeConfig,
        observer: Callable = None,
        trace_store_calls=False,
    ):
        self._sentinel_hub = sentinel_hub
        if cube_config.band_names is None:
            bands = sentinel_hub.bands(
                cube_config.dataset_name, collection_id=cube_config.collection_id
            )
            band_names = [b["name"] for b in bands]
            band_sample_types = [b.get("sampleType") for b in bands]
            d = cube_config.to_dict()
            d["band_names"] = band_names
            if all(band_sample_types):
                d["band_sample_types"] = band_sample_types
            cube_config = CubeConfig.from_dict(d)
        super().__init__(
            cube_config, observer=observer, trace_store_calls=trace_store_calls
        )

    def get_time_ranges(self) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        time_start, time_end = self._cube_config.time_range
        time_period = self._cube_config.time_period
        if time_period is not None:
            return super().get_time_ranges()

        if (
            self._cube_config.dataset_name is None
            or self._cube_config.dataset_name.upper() == "CUSTOM"
        ):
            collection_name = self._cube_config.collection_id
        else:
            collection_name = self._METADATA.dataset_collection_name(
                self._cube_config.dataset_name
            )
            if not collection_name:
                raise ValueError(
                    f"cannot find collection"
                    f" name for dataset name"
                    f" {self._cube_config.dataset_name!r}"
                )
        datetime_format = "%Y-%m-%dT%H:%M:%SZ"
        features = self._sentinel_hub.get_features(
            collection_name=collection_name,
            bbox=self._cube_config.bbox,
            crs=self._cube_config.crs,
            time_range=(
                time_start.strftime(datetime_format),
                time_end.strftime(datetime_format),
            ),
            bad_request_ok=False,
        )
        if not features:
            # Maybe the dataset has no data in given time_range
            # or the dataset has no associated time information
            # such as some BYOC or DEM.
            # Repeat query without time_range.
            features = self._sentinel_hub.get_features(
                collection_name=collection_name,
                bbox=self._cube_config.bbox,
                crs=self._cube_config.crs,
                bad_request_ok=True,
            )
            if not features:
                # Still no results.
                return []

        if not features[0]:
            # We found at least one dataset,
            # but there is no time information associated with it.
            # We now return simply the query time range.
            return [(time_start, time_end)]

        # Convert retrieved time stamps into time ranges for
        # each time slice.
        return SentinelHub.features_to_time_ranges(features)

    def get_band_encoding(self, band_name: str) -> Dict[str, Any]:
        band_sample_types = self.cube_config.band_sample_types
        if not band_sample_types:
            sample_type = self._METADATA.dataset_band_sample_type(
                self.cube_config.dataset_name, band_name, default="FLOAT32"
            )
        elif isinstance(band_sample_types, (tuple, list)):
            index = self.cube_config.band_names.index(band_name)
            sample_type = band_sample_types[index]
        else:  # isinstance(band_sample_types, str)
            sample_type = band_sample_types
        # Convert to sample type name to Zarr dtype value
        dtype = self._SAMPLE_TYPE_TO_DTYPE.get(sample_type)
        if dtype is None:
            raise TypeError(
                f"Invalid sample type {sample_type!r},"
                f" must be one of"
                f" {tuple(self._SAMPLE_TYPE_TO_DTYPE.keys())}."
            )

        band_fill_values = self.cube_config.band_fill_values
        if band_fill_values is None:
            fill_value = self._METADATA.dataset_band_fill_value(
                self.cube_config.dataset_name, band_name, default=None
            )
        elif isinstance(band_fill_values, (tuple, list)):
            index = self.cube_config.band_names.index(band_name)
            fill_value = band_fill_values[index]
        else:  # isinstance(band_fill_values, Number)
            fill_value = band_fill_values
        # Force fill value to be serializable
        if isinstance(fill_value, float):
            fill_value = float(fill_value)
            if math.isnan(fill_value):
                fill_value = None
        elif isinstance(fill_value, int):
            fill_value = int(fill_value)
        elif fill_value is not None:
            raise TypeError(
                f"Invalid fill value {fill_value!r}, must be" f" int, float, or None"
            )
        return dict(
            dtype=dtype,
            fill_value=fill_value,
            compressor=dict(id="zlib", level=8),
            order="C",
        )

    def get_band_attrs(self, band_name: str) -> Dict[str, Any]:
        band_metadata = self._METADATA.dataset_band(
            self.cube_config.dataset_name, band_name, default={}
        )
        if "fill_value" in band_metadata:
            band_metadata.pop("fill_value")
        return band_metadata

    def fetch_chunk(
        self,
        key: str,
        band_name: str,
        chunk_index: Tuple[int, ...],
        bbox: Tuple[float, float, float, float],
        time_range: Tuple[pd.Timestamp, pd.Timestamp],
    ) -> bytes:
        start_time, end_time = time_range
        time_range = start_time.isoformat(), end_time.isoformat()

        if band_name == "band_data":
            band_names = self.cube_config.band_names
        else:
            band_names = [band_name]

        band_sample_types = self.cube_config.band_sample_types
        if not band_sample_types:
            if band_name == "band_data":
                band_sample_types = [
                    SentinelHub.METADATA.dataset_band_sample_type(
                        self.cube_config.dataset_name, band_name
                    )
                    for band_name in band_names
                ]
            else:
                band_sample_types = SentinelHub.METADATA.dataset_band_sample_type(
                    self.cube_config.dataset_name, band_name
                )
        elif isinstance(band_sample_types, tuple) and band_name != "band_data":
            index = self.cube_config.band_names.index(band_name)
            band_sample_types = band_sample_types[index]

        request = SentinelHub.new_data_request(
            self.cube_config.dataset_name,
            band_names,
            self.cube_config.tile_size,
            time_range=time_range,
            bbox=bbox,
            band_sample_types=band_sample_types,
            crs=CRS_ID_TO_URI[self.cube_config.crs],
            upsampling=self.cube_config.upsampling,
            downsampling=self.cube_config.downsampling,
            mosaicking_order=self.cube_config.mosaicking_order,
            collection_id=self.cube_config.collection_id,
            band_units=self.cube_config.band_units,
            processing_kwargs=self.cube_config.processing_kwargs,
        )

        response = self._sentinel_hub.get_data(
            request, mime_type="application/octet-stream"
        )

        if response is None or not response.ok:
            message = (
                f"{key}: cannot fetch chunk for variable"
                f" {band_name!r}, bbox {bbox!r}, and"
                f" time_range {time_range!r}"
            )
            if response is not None:
                message += f": {SentinelHubError(response)}"
            raise KeyError(message)

        return response.content
