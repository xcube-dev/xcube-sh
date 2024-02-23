# Copyright © 2022-2024 by the xcube development team and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest
import zlib
from abc import ABCMeta
from abc import abstractmethod
from collections import namedtuple
from typing import Tuple, List, Dict, Any

import numpy as np
import pandas as pd
import pyproj
import xarray as xr
import zarr

from xcube_sh.chunkstore import SentinelHubChunkStore
from xcube_sh.config import CubeConfig
from xcube_sh.metadata import S2_BAND_NAMES
from xcube_sh.metadata import SentinelHubMetadata


class SentinelHubStoreTest(unittest.TestCase, metaclass=ABCMeta):
    def setUp(self) -> None:
        self.observed_kwargs = dict()

        cube_config = self.get_cube_config()

        self.assertEqual((4000, 4000), cube_config.size)
        self.assertEqual((1000, 1000), cube_config.tile_size)

        self.cube_config = cube_config
        # noinspection PyTypeChecker
        self.store = SentinelHubChunkStore(
            SentinelHubMock(cube_config),
            cube_config,
            observer=self.observe_store,
            trace_store_calls=False,
        )

    def observe_store(self, **kwargs):
        key = "{band_name}-{chunk_index}".format(**kwargs)
        self.observed_kwargs[key] = kwargs

    @abstractmethod
    def get_cube_config(self) -> CubeConfig:
        pass

    def assert_coordinate_vars(
        self, cube: xr.Dataset, expected_num_times, expected_first_3_times
    ):
        self.assertEqual(4000, cube.lon.size)
        actual = cube.lon[:4].values
        expected = np.array(
            [10.2000125, 10.2000375, 10.2000625, 10.2000875], dtype=np.float64
        )
        np.testing.assert_almost_equal(actual, expected)

        self.assertEqual(4000, cube.lat.size)
        actual = cube.lat[:4].values
        expected = np.array(
            [53.5999875, 53.5999625, 53.5999375, 53.5999125], dtype=np.float64
        )
        np.testing.assert_almost_equal(actual, expected)

        self.assertEqual(expected_num_times, cube.time.size)
        actual = cube.time[:3].values
        expected = np.array(expected_first_3_times, dtype="datetime64[ns]")
        np.testing.assert_equal(actual, expected)


class SentinelHubStore3DTest(SentinelHubStoreTest):
    def get_cube_config(self):
        return CubeConfig(
            dataset_name="S2L1C",
            band_names=["B01", "B08", "B12"],
            bbox=(10.2, 53.5, 10.3, 53.6),
            spatial_res=0.1 / 4000,
            time_range=("2017-08-01", "2017-08-31"),
            time_period="1D",
            four_d=False,
        )

    def test_basics(self):
        self.assertIn(".zmetadata", self.store)
        self.assertIn(".zgroup", self.store)
        self.assertIn(".zattrs", self.store)
        self.assertIn("B01/.zattrs", self.store)
        self.assertIn("B01/.zarray", self.store)
        self.assertIn("B01/0.0.0", self.store)

    def test_plain(self):
        # noinspection PyTypeChecker
        cube = xr.open_zarr(self.store)
        self.assert_3d_cube_is_valid(cube)

    def test_cached(self):
        store_cache = zarr.LRUStoreCache(self.store, max_size=2 * 24)
        cube = xr.open_zarr(store_cache)
        self.assert_3d_cube_is_valid(cube)

    def assert_3d_cube_is_valid(self, cube):
        cube_config = self.cube_config

        self.assertEqual(
            {"lon", "lat", "time", "time_bnds", "B01", "B08", "B12"},
            set(cube.variables),
        )

        self.assertEqual({"time": 31, "lat": 4000, "lon": 4000, "bnds": 2}, cube.sizes)

        self.assert_coordinate_vars(
            cube,
            31,
            ["2017-08-01T12:00:00", "2017-08-02T12:00:00", "2017-08-03T12:00:00"],
        )

        b01 = cube.B01
        self.assertEqual(("time", "lat", "lon"), b01.dims)
        self.assertEqual((31, 4000, 4000), b01.shape)
        self.assertEqual(((1,) * 31, (1000,) * 4, (1000,) * 4), b01.chunks)

        b01_im = b01.isel(time=2)
        self.assertEqual(("lat", "lon"), b01_im.dims)
        self.assertEqual((4000, 4000), b01_im.shape)
        self.assertEqual(((1000,) * 4, (1000,) * 4), b01_im.chunks)

        values = b01.isel(time=2).values
        self.assertEqual((4000, 4000), values.shape)

        self.assertEqual(16, len(self.observed_kwargs))
        self.assertEqual(
            [
                "B01-(2, 0, 0)",
                "B01-(2, 0, 1)",
                "B01-(2, 0, 2)",
                "B01-(2, 0, 3)",
                "B01-(2, 1, 0)",
                "B01-(2, 1, 1)",
                "B01-(2, 1, 2)",
                "B01-(2, 1, 3)",
                "B01-(2, 2, 0)",
                "B01-(2, 2, 1)",
                "B01-(2, 2, 2)",
                "B01-(2, 2, 3)",
                "B01-(2, 3, 0)",
                "B01-(2, 3, 1)",
                "B01-(2, 3, 2)",
                "B01-(2, 3, 3)",
            ],
            sorted(list(self.observed_kwargs.keys())),
        )

        x_delta = cube_config.tile_size[0] * cube_config.spatial_res
        y_delta = cube_config.tile_size[1] * cube_config.spatial_res
        bbox_200 = self.observed_kwargs["B01-(2, 0, 0)"]["bbox"]
        np.testing.assert_almost_equal(
            bbox_200, (10.2, 53.6 - y_delta, 10.2 + x_delta, 53.6)
        )
        bbox_233 = self.observed_kwargs["B01-(2, 3, 3)"]["bbox"]
        np.testing.assert_almost_equal(
            bbox_233, (10.3 - x_delta, 53.5, 10.3, 53.5 + y_delta)
        )

        self.observed_kwargs.clear()

        values = b01.isel(lon=3500, lat=1500).values
        self.assertEqual((31,), values.shape)

        self.assertEqual(31, len(self.observed_kwargs))
        self.assertEqual(
            [
                "B01-(0, 1, 3)",
                "B01-(1, 1, 3)",
                "B01-(10, 1, 3)",
                "B01-(11, 1, 3)",
                "B01-(12, 1, 3)",
                "B01-(13, 1, 3)",
                "B01-(14, 1, 3)",
                "B01-(15, 1, 3)",
                "B01-(16, 1, 3)",
                "B01-(17, 1, 3)",
                "B01-(18, 1, 3)",
                "B01-(19, 1, 3)",
                "B01-(2, 1, 3)",
                "B01-(20, 1, 3)",
                "B01-(21, 1, 3)",
                "B01-(22, 1, 3)",
                "B01-(23, 1, 3)",
                "B01-(24, 1, 3)",
                "B01-(25, 1, 3)",
                "B01-(26, 1, 3)",
                "B01-(27, 1, 3)",
                "B01-(28, 1, 3)",
                "B01-(29, 1, 3)",
                "B01-(3, 1, 3)",
                "B01-(30, 1, 3)",
                "B01-(4, 1, 3)",
                "B01-(5, 1, 3)",
                "B01-(6, 1, 3)",
                "B01-(7, 1, 3)",
                "B01-(8, 1, 3)",
                "B01-(9, 1, 3)",
            ],
            sorted(list(self.observed_kwargs.keys())),
        )


class SentinelHubStore3DTestWithAllBands(SentinelHubStoreTest):
    def get_cube_config(self):
        return CubeConfig(
            dataset_name="S2L2A",
            bbox=(10.2, 53.5, 10.3, 53.6),
            spatial_res=0.1 / 4000,
            time_range=("2017-08-01", "2017-08-31"),
            time_period=None,
            four_d=False,
        )

    def test_all_bands_available(self):
        # noinspection PyTypeChecker
        cube = xr.open_zarr(self.store)
        self.assertEqual(
            [
                "B01",
                "B02",
                "B03",
                "B04",
                "B05",
                "B06",
                "B07",
                "B08",
                "B09",
                "B10",
                "B11",
                "B12",
                "B8A",
            ],
            sorted(cube.data_vars),
        )


class SentinelHubStore3DTestWithNonDefaultCrs(SentinelHubStoreTest):
    def get_cube_config(self):
        crs_name = "EPSG:3035"
        crs = pyproj.CRS.from_string(crs_name)
        t = pyproj.Transformer.from_crs("EPSG:4326", crs, always_xy=True)
        (x1, x2), (y1, y2) = t.transform((10.2, 10.3), (53.5, 53.6))
        spatial_res = (x2 - x1) / 4000
        y2 = y1 + spatial_res * 4000
        return CubeConfig(
            dataset_name="S2L2A",
            band_names=["B01", "B08", "B12"],
            bbox=(x1, y1, x2, y2),
            crs=crs_name,
            spatial_res=spatial_res,
            time_range=("2017-08-01", "2017-08-31"),
            time_period=None,
            four_d=False,
        )

    def test_crs_info_ok(self):
        cube = xr.open_zarr(self.store)
        self.assertIn("crs", cube)
        crs_var = cube["crs"]
        crs_cf_attrs = pyproj.CRS.from_cf(crs_var.attrs)
        print(crs_cf_attrs)
        for band_name in ["B01", "B08", "B12"]:
            self.assertIn(band_name, cube)
            band = cube[band_name]
            self.assertIn("grid_mapping", band.attrs)
            self.assertEqual("crs", band.attrs["grid_mapping"])


class SentinelHubStore3DTestWithTiles(SentinelHubStoreTest):
    def get_cube_config(self):
        return CubeConfig(
            dataset_name="S2L1C",
            band_names=["B01", "B08", "B12"],
            bbox=(10.2, 53.5, 10.3, 53.6),
            spatial_res=0.1 / 4000,
            time_range=("2017-08-01", "2017-08-31"),
            time_period=None,
            four_d=False,
        )

    def test_plain(self):
        # noinspection PyTypeChecker
        cube = xr.open_zarr(self.store)
        self.assert_3d_cube_is_valid(cube)

    def test_cached(self):
        store_cache = zarr.LRUStoreCache(self.store, max_size=2 * 24)
        cube = xr.open_zarr(store_cache)
        self.assert_3d_cube_is_valid(cube)

    def assert_3d_cube_is_valid(self, cube):
        cube_config = self.cube_config

        self.assertEqual(
            {"lon", "lat", "time", "time_bnds", "B01", "B08", "B12"},
            set(cube.variables),
        )

        self.assertEqual({"time": 15, "lat": 4000, "lon": 4000, "bnds": 2}, cube.sizes)

        self.assert_coordinate_vars(
            cube,
            15,
            ["2017-08-01T08:00:00", "2017-08-03T08:00:00", "2017-08-05T08:00:00"],
        )

        b01 = cube.B01
        self.assertEqual(("time", "lat", "lon"), b01.dims)
        self.assertEqual((15, 4000, 4000), b01.shape)
        self.assertEqual(((1,) * 15, (1000,) * 4, (1000,) * 4), b01.chunks)

        b01_im = b01.isel(time=2)
        self.assertEqual(("lat", "lon"), b01_im.dims)
        self.assertEqual((4000, 4000), b01_im.shape)
        self.assertEqual(((1000,) * 4, (1000,) * 4), b01_im.chunks)

        values = b01.isel(time=2).values
        self.assertEqual((4000, 4000), values.shape)

        self.assertEqual(16, len(self.observed_kwargs))
        self.assertEqual(
            [
                "B01-(2, 0, 0)",
                "B01-(2, 0, 1)",
                "B01-(2, 0, 2)",
                "B01-(2, 0, 3)",
                "B01-(2, 1, 0)",
                "B01-(2, 1, 1)",
                "B01-(2, 1, 2)",
                "B01-(2, 1, 3)",
                "B01-(2, 2, 0)",
                "B01-(2, 2, 1)",
                "B01-(2, 2, 2)",
                "B01-(2, 2, 3)",
                "B01-(2, 3, 0)",
                "B01-(2, 3, 1)",
                "B01-(2, 3, 2)",
                "B01-(2, 3, 3)",
            ],
            sorted(list(self.observed_kwargs.keys())),
        )

        x_delta = cube_config.tile_size[0] * cube_config.spatial_res
        y_delta = cube_config.tile_size[1] * cube_config.spatial_res
        bbox_200 = self.observed_kwargs["B01-(2, 0, 0)"]["bbox"]
        np.testing.assert_almost_equal(
            bbox_200, (10.2, 53.6 - y_delta, 10.2 + x_delta, 53.6)
        )
        bbox_233 = self.observed_kwargs["B01-(2, 3, 3)"]["bbox"]
        np.testing.assert_almost_equal(
            bbox_233, (10.3 - x_delta, 53.5, 10.3, 53.5 + y_delta)
        )

        self.observed_kwargs.clear()

        values = b01.isel(lon=3500, lat=1500).values
        self.assertEqual((15,), values.shape)

        self.assertEqual(15, len(self.observed_kwargs))
        self.assertEqual(
            [
                "B01-(0, 1, 3)",
                "B01-(1, 1, 3)",
                "B01-(10, 1, 3)",
                "B01-(11, 1, 3)",
                "B01-(12, 1, 3)",
                "B01-(13, 1, 3)",
                "B01-(14, 1, 3)",
                "B01-(2, 1, 3)",
                "B01-(3, 1, 3)",
                "B01-(4, 1, 3)",
                "B01-(5, 1, 3)",
                "B01-(6, 1, 3)",
                "B01-(7, 1, 3)",
                "B01-(8, 1, 3)",
                "B01-(9, 1, 3)",
            ],
            sorted(list(self.observed_kwargs.keys())),
        )


# noinspection PyTypeChecker
class SentinelHubStore3DTestWithFillValue(unittest.TestCase):
    default_kwargs = dict(
        dataset_name="S2L1C",
        band_names=["B01", "B08", "B12"],
        bbox=(10.2, 53.5, 10.3, 53.6),
        spatial_res=0.1 / 4000,
        time_range=("2017-08-01", "2017-08-31"),
        time_period="1D",
    )

    def test_all_zero(self):
        cube_config = CubeConfig(band_fill_values=0, **self.default_kwargs)
        self.store = SentinelHubChunkStore(SentinelHubMock(cube_config), cube_config)
        cube = xr.open_zarr(self.store)
        self.assertEqual(0, cube.B01.encoding.get("_FillValue"))
        self.assertEqual(0, cube.B08.encoding.get("_FillValue"))
        self.assertEqual(0, cube.B12.encoding.get("_FillValue"))

    def test_individual(self):
        cube_config = CubeConfig(band_fill_values=[1, 2, 3], **self.default_kwargs)
        self.store = SentinelHubChunkStore(SentinelHubMock(cube_config), cube_config)
        cube = xr.open_zarr(self.store)
        self.assertEqual(1, cube.B01.encoding.get("_FillValue"))
        self.assertEqual(2, cube.B08.encoding.get("_FillValue"))
        self.assertEqual(3, cube.B12.encoding.get("_FillValue"))

    def test_individual_nan(self):
        cube_config = CubeConfig(
            band_fill_values=[np.nan, np.nan, np.nan], **self.default_kwargs
        )
        self.store = SentinelHubChunkStore(SentinelHubMock(cube_config), cube_config)
        cube = xr.open_zarr(self.store)
        self.assertEqual(None, cube.B01.encoding.get("_FillValue"))
        self.assertEqual(None, cube.B08.encoding.get("_FillValue"))
        self.assertEqual(None, cube.B12.encoding.get("_FillValue"))


class SentinelHubStore4DTest(SentinelHubStoreTest):
    def get_cube_config(self):
        return CubeConfig(
            dataset_name="S2L1C",
            band_names=["B01", "B08", "B12"],
            bbox=(10.2, 53.5, 10.3, 53.6),
            spatial_res=0.1 / 4000,
            time_range=("2017-08-01", "2017-08-31"),
            time_period="1D",
            four_d=True,
        )

    def test_plain(self):
        cube = xr.open_zarr(self.store)
        self.assert_4d_cube_is_valid(cube)

    def test_cached(self):
        store_cache = zarr.LRUStoreCache(self.store, max_size=2 * 24)
        cube = xr.open_zarr(store_cache)
        self.assert_4d_cube_is_valid(cube)

    def assert_4d_cube_is_valid(self, cube):
        cube_config = self.cube_config

        self.assertEqual(
            {"lon", "lat", "time", "time_bnds", "band", "band_data"},
            set(cube.variables),
        )

        self.assertEqual(
            {"time": 31, "lat": 4000, "lon": 4000, "bnds": 2, "band": 3}, cube.sizes
        )

        self.assert_coordinate_vars(
            cube,
            31,
            ["2017-08-01T12:00:00", "2017-08-02T12:00:00", "2017-08-03T12:00:00"],
        )

        band_data = cube.band_data
        self.assertEqual(("time", "lat", "lon", "band"), band_data.dims)
        self.assertEqual((31, 4000, 4000, 3), band_data.shape)
        self.assertEqual(((1,) * 31, (1000,) * 4, (1000,) * 4, (3,)), band_data.chunks)

        band_data_im = band_data.isel(time=2, band=1)
        self.assertEqual(("lat", "lon"), band_data_im.dims)
        self.assertEqual((4000, 4000), band_data_im.shape)
        self.assertEqual(((1000,) * 4, (1000,) * 4), band_data_im.chunks)

        values = band_data_im.values
        self.assertEqual((4000, 4000), values.shape)

        self.assertEqual(16, len(self.observed_kwargs))
        self.assertEqual(
            [
                "band_data-(2, 0, 0, 0)",
                "band_data-(2, 0, 1, 0)",
                "band_data-(2, 0, 2, 0)",
                "band_data-(2, 0, 3, 0)",
                "band_data-(2, 1, 0, 0)",
                "band_data-(2, 1, 1, 0)",
                "band_data-(2, 1, 2, 0)",
                "band_data-(2, 1, 3, 0)",
                "band_data-(2, 2, 0, 0)",
                "band_data-(2, 2, 1, 0)",
                "band_data-(2, 2, 2, 0)",
                "band_data-(2, 2, 3, 0)",
                "band_data-(2, 3, 0, 0)",
                "band_data-(2, 3, 1, 0)",
                "band_data-(2, 3, 2, 0)",
                "band_data-(2, 3, 3, 0)",
            ],
            sorted(list(self.observed_kwargs.keys())),
        )

        x_delta = cube_config.tile_size[0] * cube_config.spatial_res
        y_delta = cube_config.tile_size[1] * cube_config.spatial_res
        bbox_2000 = self.observed_kwargs["band_data-(2, 0, 0, 0)"]["bbox"]
        np.testing.assert_almost_equal(
            bbox_2000, (10.2, 53.6 - y_delta, 10.2 + x_delta, 53.6)
        )
        bbox_2330 = self.observed_kwargs["band_data-(2, 3, 3, 0)"]["bbox"]
        np.testing.assert_almost_equal(
            bbox_2330, (10.3 - x_delta, 53.5, 10.3, 53.5 + y_delta)
        )

        self.observed_kwargs.clear()

        values = band_data.isel(lon=3500, lat=1500, band=1).values
        self.assertEqual((31,), values.shape)

        self.assertEqual(31, len(self.observed_kwargs))
        self.assertEqual(
            [
                "band_data-(0, 1, 3, 0)",
                "band_data-(1, 1, 3, 0)",
                "band_data-(10, 1, 3, 0)",
                "band_data-(11, 1, 3, 0)",
                "band_data-(12, 1, 3, 0)",
                "band_data-(13, 1, 3, 0)",
                "band_data-(14, 1, 3, 0)",
                "band_data-(15, 1, 3, 0)",
                "band_data-(16, 1, 3, 0)",
                "band_data-(17, 1, 3, 0)",
                "band_data-(18, 1, 3, 0)",
                "band_data-(19, 1, 3, 0)",
                "band_data-(2, 1, 3, 0)",
                "band_data-(20, 1, 3, 0)",
                "band_data-(21, 1, 3, 0)",
                "band_data-(22, 1, 3, 0)",
                "band_data-(23, 1, 3, 0)",
                "band_data-(24, 1, 3, 0)",
                "band_data-(25, 1, 3, 0)",
                "band_data-(26, 1, 3, 0)",
                "band_data-(27, 1, 3, 0)",
                "band_data-(28, 1, 3, 0)",
                "band_data-(29, 1, 3, 0)",
                "band_data-(3, 1, 3, 0)",
                "band_data-(30, 1, 3, 0)",
                "band_data-(4, 1, 3, 0)",
                "band_data-(5, 1, 3, 0)",
                "band_data-(6, 1, 3, 0)",
                "band_data-(7, 1, 3, 0)",
                "band_data-(8, 1, 3, 0)",
                "band_data-(9, 1, 3, 0)",
            ],
            sorted(list(self.observed_kwargs.keys())),
        )


MockResponse = namedtuple("Response", ["ok", "status_code", "headers", "content"])


class SentinelHubMock:
    METADATA = SentinelHubMetadata()

    def __init__(self, config: CubeConfig):
        self._config = config
        self._requests = []

    def band_names(self, dataset_name: str, collection_id=None):
        return S2_BAND_NAMES

    def bands(self, dataset_name: str, collection_id=None):
        return [dict(name=b) for b in S2_BAND_NAMES]

    # noinspection PyUnusedLocal
    def get_features(
        self,
        collection_name: str,
        bbox: Tuple[float, float, float, float] = None,
        crs: str = None,
        time_range: Tuple[str, str] = None,
        bad_request_ok: bool = False,
    ) -> List[Dict[str, Any]]:
        """Return dummy catalog features"""
        start_time, end_time = map(pd.to_datetime, time_range)
        datetime = start_time + pd.to_timedelta("8H")
        index = 0
        features = []
        while datetime < end_time:
            index += 1
            datetime_str = datetime.isoformat()
            if datetime_str.endswith("+00:00"):
                datetime_str = datetime_str[: -len("+00:00")] + "Z"
            feature = dict(properties=dict(datetime=datetime_str))
            features.append(feature)
            datetime += pd.to_timedelta("48H")
        return features

    # noinspection PyUnusedLocal
    def get_data(self, request, mime_type=None):
        """
        Return zlib (level 8) compressed float32 zeros.
        """
        self._requests.append(request)

        chunk_width, chunk_height = self._config.tile_size
        num_bands = len(self._config.band_names)

        if self._config.four_d:
            chunk_shape = chunk_height, chunk_width, num_bands
        else:
            chunk_shape = chunk_height, chunk_width

        zero_chunk_array = np.zeros(chunk_shape, dtype=np.float32)
        content = zlib.compress(bytes(zero_chunk_array), level=8)

        headers = {
            "SH-Width": chunk_width,
            "SH-Height": chunk_height,
            "SH-Components": num_bands,
            "SH-SampleType": "FLOAT32",
        }

        return MockResponse(ok=True, status_code=200, headers=headers, content=content)
