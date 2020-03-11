# The MIT License (MIT)
# Copyright (c) 2019 by the xcube development team and contributors
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

import math
import warnings
from typing import Tuple, Union, Optional, Sequence, Dict, Any

import pandas as pd

from .constants import DEFAULT_CRS, DEFAULT_TIME_TOLERANCE
from .constants import SH_MAX_IMAGE_SIZE, DEFAULT_TILE_SIZE


def _safe_int_div(x: int, y: int) -> int:
    return (x + y - 1) // y


class CubeConfig:
    """
    Sentinel Hub cube configuration.

    :param dataset_name: Dataset name. Mandatory.
    :param band_names: Band names. Mandatory.
    :param band_units: Band units. Optional.
    :param band_sample_types: Band sample types. Optional.
    :param tile_size: Tile size as tuple (width, height). Optional.
    :param chunk_size: Deprecated. Use *tile_size*.
    :param geometry:
    :param spatial_res:
    :param crs:
    :param time_range:
    :param time_period:
    :param time_tolerance:
    :param collection_id:
    :param four_d:
    :param exception_type:
    """

    def __init__(self,
                 dataset_name: str = None,
                 band_names: Sequence[str] = None,
                 band_units: Union[str, Sequence[str]] = None,
                 band_sample_types: Union[str, Sequence[str]] = None,
                 tile_size: Union[str, Tuple[int, int]] = None,
                 chunk_size: Union[str, Tuple[int, int]] = None,
                 geometry: Union[str, Tuple[float, float, float, float]] = None,
                 spatial_res: float = None,
                 crs: str = None,
                 time_range: Union[str, pd.Timestamp, Tuple[str, str], Tuple[pd.Timestamp, pd.Timestamp]] = None,
                 time_period: Union[str, pd.Timedelta] = None,
                 time_tolerance: Union[str, pd.Timedelta] = None,
                 collection_id: str = None,
                 four_d: bool = False,
                 exception_type=ValueError):

        if not dataset_name:
            raise exception_type('dataset name must be given')
        if not geometry:
            raise exception_type('geometry must be given')
        if spatial_res is None:
            raise exception_type('spatial resolution must be given')
        if spatial_res <= 0.0:
            raise exception_type('spatial resolution must be a positive number')
        if not band_names:
            raise exception_type('band names must be a given')
        if not crs:
            raise exception_type('CRS must be a given')
        if not time_range:
            raise exception_type('time range must be given')

        if isinstance(geometry, str):
            x1, y1, x2, y2 = tuple(map(float, geometry.split(',', maxsplit=3)))
        else:
            x1, y1, x2, y2 = geometry

        crs = crs or DEFAULT_CRS
        time_tolerance = time_tolerance or DEFAULT_TIME_TOLERANCE

        if chunk_size is not None:
            warnings.warn('the chunk_size parameter is no longer supported, use tile_size instead')
            if tile_size is None:
                tile_size = chunk_size

        width, height = (max(1, round((x2 - x1) / spatial_res)),
                         max(1, round((y2 - y1) / spatial_res)))

        if tile_size is None:
            tile_width, tile_height = None, None
        elif isinstance(tile_size, str):
            parsed = tuple(map(int, geometry.split(',', maxsplit=1)))
            if len(parsed) == 1:
                tile_width, tile_height = parsed[0], parsed[0]
            elif len(parsed) == 2:
                tile_width, tile_height = parsed
            else:
                raise exception_type(f'invalid tile size: {tile_size}')
        else:
            tile_width, tile_height = tile_size
        if tile_width is None and tile_height is None:
            num_pixels_per_tile = DEFAULT_TILE_SIZE * DEFAULT_TILE_SIZE
            tile_width = math.ceil(math.sqrt(width * num_pixels_per_tile / height))
            tile_height = (num_pixels_per_tile + tile_width - 1) // tile_width
        elif tile_width is None:
            tile_width = tile_height
        elif tile_height is None:
            tile_height = tile_width
        if tile_width > SH_MAX_IMAGE_SIZE:
            tile_width = SH_MAX_IMAGE_SIZE
        if tile_height > SH_MAX_IMAGE_SIZE:
            tile_height = SH_MAX_IMAGE_SIZE

        if width < 1.5 * tile_width:
            tile_width = width
        else:
            width = self._adjust_size(width, tile_width)
        if height < 1.5 * tile_height:
            tile_height = height
        else:
            height = self._adjust_size(height, tile_height)

        x2, y2 = x1 + width * spatial_res, y1 + height * spatial_res

        geometry = x1, y1, x2, y2

        if isinstance(time_range, str):
            time_range = tuple(map(lambda s: s.strip(),
                                   time_range.split(',', maxsplit=1) if ',' in time_range else (
                                       time_range, time_range)))
            time_range = tuple(time_range)
        if len(time_range) == 1:
            time_range = time_range + time_range
        if len(time_range) != 2:
            exception_type('Time range must be have two elements')

        start_time, end_time = tuple(time_range)
        if isinstance(start_time, str) or isinstance(end_time, str):
            def convert_time(time_str):
                return pd.to_datetime(time_str, utc=True)

            start_time, end_time = tuple(map(convert_time, time_range))

        time_range = start_time, end_time

        time_period = time_period or None
        if isinstance(time_period, str):
            time_period = pd.to_timedelta(time_period)

        time_tolerance = time_tolerance or None
        if isinstance(time_tolerance, str):
            time_tolerance = pd.to_timedelta(time_tolerance)

        self._dataset_name = dataset_name
        self._band_names = tuple(band_names)
        self._band_units = band_units or None
        self._band_sample_types = band_sample_types or None
        self._geometry = geometry
        self._spatial_res = spatial_res
        self._crs = crs
        self._time_range = time_range
        self._time_period = time_period
        self._time_tolerance = time_tolerance
        self._collection_id = collection_id
        self._four_d = four_d
        self._size = width, height
        self._tile_size = tile_width, tile_height
        self._num_tiles = width // tile_width, height // tile_height

    @classmethod
    def from_dict(cls, cube_config_dict: Dict[str, Any], exception_type=ValueError) -> 'CubeConfig':
        code = CubeConfig.__init__.__code__
        valid_keywords = set(code.co_varnames[1: code.co_argcount])
        given_keywords = set(cube_config_dict.keys())
        for keyword in cube_config_dict.keys():
            if keyword in valid_keywords:
                given_keywords.remove(keyword)
        if len(given_keywords) == 1:
            raise exception_type(f'Found invalid parameter {given_keywords.pop()!r} in cube configuration')
        elif len(given_keywords) > 1:
            given_keywords_text = ', '.join(map(lambda s: f'{s!r}', given_keywords))
            raise exception_type(f'Found invalid parameters in cube configuration: {given_keywords_text}')
        return CubeConfig(exception_type=exception_type, **cube_config_dict)

    def as_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dictionary that can be passed to ctor as kwargs"""
        time_range = (self.time_range[0].isoformat(), self.time_range[1].isoformat()) \
            if self.time_range else None
        time_period = str(self.time_period) \
            if self.time_period else None
        time_tolerance = str(self.time_tolerance) \
            if self.time_tolerance else None
        return dict(dataset_name=self.dataset_name,
                    band_names=self.band_names,
                    band_units=self.band_units,
                    band_sample_types=self.band_sample_types,
                    tile_size=self.tile_size,
                    geometry=self.geometry,
                    spatial_res=self.spatial_res,
                    crs=self.crs,
                    time_range=time_range,
                    time_period=time_period,
                    time_tolerance=time_tolerance,
                    collection_id=self.collection_id,
                    four_d=self.four_d)

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def band_names(self) -> Tuple[str, ...]:
        return self._band_names

    @property
    def band_units(self) -> Union[None, str, Tuple[str, ...]]:
        return self._band_units

    @property
    def band_sample_types(self) -> Union[None, str, Tuple[str, ...]]:
        return self._band_sample_types

    @property
    def crs(self) -> str:
        return self._crs

    @property
    def geometry(self) -> Tuple[float, float, float, float]:
        return self._geometry

    @property
    def spatial_res(self) -> float:
        return self._spatial_res

    @property
    def time_range(self) -> Tuple[pd.Timestamp, pd.Timestamp]:
        return self._time_range

    @property
    def time_period(self) -> Optional[pd.Timedelta]:
        return self._time_period

    @property
    def time_tolerance(self) -> Optional[pd.Timedelta]:
        return self._time_tolerance

    @property
    def collection_id(self) -> Optional[str]:
        return self._collection_id

    @property
    def four_d(self) -> bool:
        return self._four_d

    @property
    def size(self) -> Tuple[int, int]:
        return self._size

    @property
    def tile_size(self) -> Tuple[int, int]:
        return self._tile_size

    @property
    def num_tiles(self) -> Tuple[int, int]:
        return self._num_tiles

    @property
    def is_wgs84_crs(self) -> bool:
        return self._crs.endswith('/4326') or self._crs.endswith('/WGS84')

    @classmethod
    def _adjust_size(cls, size: int, tile_size: int) -> int:
        if size > tile_size:
            num_tiles = _safe_int_div(size, tile_size)
            size = num_tiles * tile_size
        return size
