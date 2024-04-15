# Copyright © 2022-2024 by the xcube development team and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import math
import warnings
from datetime import datetime
from typing import Tuple, Union, Optional, Sequence, Dict, Any
from numbers import Number

import pandas as pd

from xcube.util.assertions import assert_given, assert_true, assert_in
from .constants import CRS_ID_TO_URI
from .constants import CRS_URI_TO_ID
from .constants import DEFAULT_CRS
from .constants import DEFAULT_MOSAICKING_ORDER
from .constants import DEFAULT_RESAMPLING
from .constants import DEFAULT_TILE_SIZE
from .constants import DEFAULT_TIME_TOLERANCE
from .constants import MOSAICKING_ORDERS
from .constants import RESAMPLINGS
from .constants import SH_MAX_IMAGE_SIZE


def _safe_int_div(x: int, y: int) -> int:
    return (x + y - 1) // y


Bbox = Tuple[float, float, float, float]
TimeRange = Union[
    str,
    pd.Timestamp,
    Tuple[Union[None, str, pd.Timestamp], Union[None, str, pd.Timestamp]],
]


class CubeConfig:
    """
    Sentinel Hub cube configuration.

    :param dataset_name: Dataset name. If *collection_id* is given,
        *dataset_name* must be omitted or set to "CUSTOM".
    :param band_names: Optional sequence of band names. If omitted (=None)
        all bands are included.
    :param band_fill_values: Band fill values. Optional.
    :param band_sample_types: Band sample types. Optional.
    :param band_units: Band units. Optional.
    :param tile_size: Tile size as tuple (width, height). Optional.
    :param chunk_size: Deprecated. Use *tile_size*.
    :param bbox: tuple of 4 numbers: (x1, y1, x2, y2)
    :param geometry: Deprecated. Use *bbox*.
    :param spatial_res: Spatial resolution. Must be > 0.
    :param crs: Coordinate reference system. If None, original source
        CRS will be used.
    :param upsampling: Spatial upsampling method.
        Must be one of 'NEAREST', 'BILINEAR', 'BICUBIC'.
        Defaults to 'NEAREST'.
    :param downsampling: Spatial downsampling method.
        Must be one of 'NEAREST', 'BILINEAR', 'BICUBIC'.
        Defaults to 'NEAREST'.
    :param mosaicking_order: Order in which observations are
        temporarilly aggregated.
        Must be one of 'mostRecent', 'leastRecent', 'leastCC'.
        Defaults to 'mostRecent'.
    :param time_range: Time range tuple; (start time, end time).
    :param time_period: A string denoting the temporal aggregation perriod,
        such as "8D", "1W", "2W".
        If None, all observations are included.
    :param time_tolerance: The tolerance used to identify whether a dataset
        should still be included within a time period.
    :param collection_id: Extra identifier used to identity a BYOC dataset.
        If given, *dataset_name* must be omitted or set to "CUSTOM".
    :param four_d: If variables should appear as forth dimension rather
        than separate arrays.
    :param exception_type: The type of exception to be raised on error
    :param processing_kwargs: Processing Keywords Arguments to be passed to Sentinel Hub
    such as for S1GRD -> {
                    "orthorectify": "false",
                    "backCoeff": "GAMMA0_ELLIPSOID",
                    "speckleFilter": {
                        "type": "LEE",
                        "windowSizeX": 5,
                        "windowSizeY": 5
                    }
    defaults to None
    """

    def __init__(
        self,
        dataset_name: str = None,
        band_names: Sequence[str] = None,
        band_sample_types: Union[str, Sequence[str]] = None,
        band_fill_values: Union[Number, Sequence[Number]] = None,
        band_units: Union[str, Sequence[str]] = None,
        tile_size: Union[str, Tuple[int, int]] = None,
        chunk_size: Union[str, Tuple[int, int]] = None,
        bbox: Bbox = None,
        geometry: Union[str, Tuple[float, float, float, float]] = None,
        spatial_res: float = None,
        crs: str = None,
        upsampling: str = None,
        downsampling: str = None,
        mosaicking_order: str = None,
        time_range: TimeRange = None,
        time_period: Union[str, pd.Timedelta] = None,
        time_tolerance: Union[str, pd.Timedelta] = None,
        collection_id: str = None,
        four_d: bool = False,
        exception_type=ValueError,
        processing_kwargs: dict = None,
    ):
        crs = crs or DEFAULT_CRS
        if crs in CRS_URI_TO_ID:
            crs = CRS_URI_TO_ID[crs]
        assert_true(crs in CRS_ID_TO_URI, "invalid crs")

        upsampling = upsampling or DEFAULT_RESAMPLING
        assert_in(upsampling, RESAMPLINGS, "upsampling")

        downsampling = downsampling or DEFAULT_RESAMPLING
        assert_in(downsampling, RESAMPLINGS, "downsampling")

        mosaicking_order = mosaicking_order or DEFAULT_MOSAICKING_ORDER
        assert_in(mosaicking_order, MOSAICKING_ORDERS, "mosaicking_order")

        if collection_id and not collection_id.startswith("byoc-"):
            collection_id = f"byoc-{collection_id}"

        if not dataset_name:
            assert_given(collection_id, "collection_id")
            dataset_name = "CUSTOM"
        if collection_id:
            assert_true(
                dataset_name.upper() == "CUSTOM", 'dataset_name must be "CUSTOM"'
            )

        assert_given(spatial_res, "spatial_res")
        assert_true(spatial_res > 0.0, "spatial_res must be a positive number")

        assert_true(not (geometry and bbox), "geometry and bbox cannot both be given")
        if geometry is not None:
            warnings.warn(
                "the geometry parameter is no longer " "supported, use bbox instead"
            )
            if bbox is None:
                bbox = geometry
                geometry = None
        assert_given(bbox, "bbox")

        assert_given(time_range, "time_range")

        start_date, end_date = time_range if time_range is not None else (None, None)
        start_date = start_date if start_date is not None else "1970-01-01"
        end_date = (
            end_date if end_date is not None else datetime.now().strftime("%Y-%m-%d")
        )
        time_range = start_date, end_date
        time_period = time_period or None
        time_tolerance = time_tolerance or None

        if time_period is None and time_tolerance is None:
            time_tolerance = DEFAULT_TIME_TOLERANCE

        try:
            if isinstance(bbox, str):
                x1, y1, x2, y2 = tuple(map(float, bbox.split(",", maxsplit=3)))
            else:
                x1, y1, x2, y2 = bbox
        except (TypeError, ValueError):
            raise ValueError("bbox must be a tuple of 4 numbers")

        if chunk_size is not None:
            warnings.warn(
                "the chunk_size parameter is no "
                "longer supported, use tile_size instead"
            )
            if tile_size is None:
                tile_size = chunk_size

        width, height = (
            max(1, round((x2 - x1) / spatial_res)),
            max(1, round((y2 - y1) / spatial_res)),
        )

        if tile_size is None:
            tile_width, tile_height = None, None
        elif isinstance(tile_size, str):
            parsed = tuple(map(int, geometry.split(",", maxsplit=1)))
            if len(parsed) == 1:
                tile_width, tile_height = parsed[0], parsed[0]
            elif len(parsed) == 2:
                tile_width, tile_height = parsed
            else:
                raise exception_type(f"invalid tile size: {tile_size}")
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

        bbox = x1, y1, x2, y2

        if isinstance(time_range, str):
            time_range = tuple(
                map(
                    lambda s: s.strip(),
                    time_range.split(",", maxsplit=1)
                    if "," in time_range
                    else (time_range, time_range),
                )
            )
            time_range = tuple(time_range)
        if len(time_range) == 1:
            time_range = time_range + time_range
        if len(time_range) != 2:
            exception_type("Time range must be have two elements")

        start_time, end_time = tuple(time_range)
        if isinstance(start_time, str) or isinstance(end_time, str):

            def convert_time(time_str):
                return pd.to_datetime(time_str, utc=True)

            start_time, end_time = tuple(map(convert_time, time_range))

        time_range = start_time, end_time

        if isinstance(time_period, str):
            time_period = pd.to_timedelta(time_period)

        if isinstance(time_tolerance, str):
            time_tolerance = pd.to_timedelta(time_tolerance)

        self._dataset_name = dataset_name
        self._band_names = tuple(band_names) if band_names is not None else None
        self._band_fill_values = band_fill_values
        self._band_sample_types = band_sample_types or None
        self._band_units = band_units or None
        self._bbox = bbox
        self._spatial_res = spatial_res
        self._crs = crs
        self._upsampling = upsampling
        self._downsampling = downsampling
        self._mosaicking_order = mosaicking_order
        self._time_range = time_range
        self._time_period = time_period
        self._time_tolerance = time_tolerance
        self._collection_id = collection_id
        self._four_d = four_d
        self._size = width, height
        self._tile_size = tile_width, tile_height
        self._num_tiles = width // tile_width, height // tile_height
        self._processing_kwargs = processing_kwargs

    @classmethod
    def from_dict(
        cls, cube_config_dict: Dict[str, Any], exception_type=ValueError
    ) -> "CubeConfig":
        code = CubeConfig.__init__.__code__
        valid_keywords = set(code.co_varnames[1 : code.co_argcount])
        given_keywords = set(cube_config_dict.keys())
        for keyword in cube_config_dict.keys():
            if keyword in valid_keywords:
                given_keywords.remove(keyword)
        if len(given_keywords) == 1:
            raise exception_type(
                f"Found invalid parameter "
                f"{given_keywords.pop()!r} in "
                f"cube configuration"
            )
        elif len(given_keywords) > 1:
            given_keywords_text = ", ".join(
                map(lambda s: f"{s!r}", sorted(given_keywords))
            )
            raise exception_type(
                f"Found invalid parameters in "
                f"cube configuration: {given_keywords_text}"
            )
        return CubeConfig(exception_type=exception_type, **cube_config_dict)

    def as_dict(self) -> Dict[str, Any]:
        """
        Deprecated. Use to_dict() instead.
        """
        warnings.warn(
            "as_dict() has been deprecated. Use to_dict() instead.", DeprecationWarning
        )
        return self.to_dict()

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to JSON-serializable dictionary
        that can be passed to ctor as kwargs.
        """
        time_range = (
            (self.time_range[0].isoformat(), self.time_range[1].isoformat())
            if self.time_range
            else None
        )
        time_period = str(self.time_period) if self.time_period else None
        time_tolerance = str(self.time_tolerance) if self.time_tolerance else None
        return dict(
            dataset_name=self.dataset_name,
            band_names=self.band_names,
            band_fill_values=self.band_fill_values,
            band_sample_types=self.band_sample_types,
            band_units=self.band_units,
            tile_size=self.tile_size,
            bbox=self.bbox,
            spatial_res=self.spatial_res,
            crs=self.crs,
            upsampling=self.upsampling,
            downsampling=self.downsampling,
            mosaicking_order=self.mosaicking_order,
            time_range=time_range,
            time_period=time_period,
            time_tolerance=time_tolerance,
            collection_id=self.collection_id,
            four_d=self.four_d,
        )

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def variable_names(self) -> Optional[Tuple[str, ...]]:
        """Alias for *band_names*."""
        return self.band_names

    @property
    def band_names(self) -> Optional[Tuple[str, ...]]:
        return self._band_names

    @property
    def band_sample_types(self) -> Union[None, str, Sequence[str]]:
        return self._band_sample_types

    @property
    def band_fill_values(self) -> Union[None, Number, Sequence[Number]]:
        return self._band_fill_values

    @property
    def band_units(self) -> Union[None, str, Sequence[str]]:
        return self._band_units

    @property
    def crs(self) -> str:
        return self._crs

    @property
    def bbox(self) -> Tuple[float, float, float, float]:
        return self._bbox

    @property
    def geometry(self) -> Tuple[float, float, float, float]:
        return self.bbox

    @property
    def spatial_res(self) -> float:
        return self._spatial_res

    @property
    def upsampling(self) -> str:
        return self._upsampling

    @property
    def downsampling(self) -> str:
        return self._downsampling

    @property
    def mosaicking_order(self) -> str:
        return self._mosaicking_order

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
    def is_geographic_crs(self) -> bool:
        return self._crs in ("CRS84", "WGS84", "EPSG:4326")

    @property
    def is_wgs84_crs(self) -> bool:
        """Deprecated."""
        return self.is_geographic_crs

    @classmethod
    def _adjust_size(cls, size: int, tile_size: int) -> int:
        if size > tile_size:
            num_tiles = _safe_int_div(size, tile_size)
            size = num_tiles * tile_size
        return size

    @property
    def processing_kwargs(self) -> dict:
        return self._processing_kwargs
