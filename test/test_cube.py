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

import collections.abc
import unittest

import numpy as np
import xarray as xr

from test.test_sentinelhub import HAS_SH_CREDENTIALS
from test.test_sentinelhub import REQUIRE_SH_CREDENTIALS
from xcube_sh.config import CubeConfig
from xcube_sh.cube import open_cube
from xcube_sh.sentinelhub import SentinelHub

cube_config = CubeConfig(dataset_name='S2L1C',
                         band_names=['B04'],
                         bbox=(10.00, 54.27, 11.00, 54.60),
                         spatial_res=0.00018,
                         time_range=('2018-05-14', '2018-07-31'),
                         time_tolerance='30M')

cube_config_t1_none = CubeConfig(dataset_name='S2L1C',
                                 band_names=['B04'],
                                 bbox=(10.00, 54.27, 11.00, 54.60),
                                 spatial_res=0.00018,
                                 time_range=('2021-01-01', None),
                                 time_period='1D')

cube_config_t_none = CubeConfig(dataset_name='S2L1C',
                                band_names=['B04'],
                                bbox=(10.00, 54.27, 11.00, 54.60),
                                spatial_res=0.00018,
                                time_range=(None, '2021-02-18'),
                                time_tolerance='30M')

cube_config_with_crs = CubeConfig(dataset_name='S2L1C',
                                  band_names=['B01'],
                                  tile_size=(512, 512),
                                  bbox=(-1953275.571528, 1648364.470634, -1936149.179188, 1664301.688856),
                                  crs="http://www.opengis.net/def/crs/EPSG/0/3857",
                                  spatial_res=10,  # in meters
                                  time_range=('2018-05-14', '2020-07-31'),
                                  time_tolerance='30M')

cube_config_LOTL2 = CubeConfig(dataset_name='LOTL2',
                               band_names=['B02', 'B03'],
                               bbox=(-17.554176, 14.640112, -17.387367, 14.792487),
                               spatial_res=0.000089,
                               time_range=('2018-05-14', '2020-07-31'),
                               time_tolerance='30M')

cube_config_S2L2A = CubeConfig(dataset_name='S2L2A',
                               band_names=['B03', 'B08', 'CLM'],
                               bbox=(2894267.8988124575, 9262943.968658403, 2899443.8488556934, 9268505.554239485),
                               crs="http://www.opengis.net/def/crs/EPSG/0/3857",
                               spatial_res=10,
                               time_range=('2020-06-01', '2020-06-30'),
                               )

cube_config_S2L2A_1D = CubeConfig(dataset_name='S2L2A',
                                  band_names=['B03', 'B08', 'CLM'],
                                  bbox=(2894267.8988124575, 9262943.968658403, 2899443.8488556934, 9268505.554239485),
                                  crs="http://www.opengis.net/def/crs/EPSG/0/3857",
                                  spatial_res=10,
                                  time_range=('2020-06-01', '2020-06-30'),
                                  time_period='1D'
                                  )

cube_config_S2L2A_WGS84 = CubeConfig(dataset_name='S2L2A',
                                     band_names=['B03', 'B08', 'CLM'],
                                     bbox=(25.99965089839723, 63.65600798545179, 26.046183630114623, 63.67816348259773),
                                     spatial_res=0.0001,
                                     time_range=('2020-06-01', '2020-06-30'),
                                     )

cube_config_S2L2A_WGS84_1D = CubeConfig(dataset_name='S2L2A',
                                        band_names=['B03', 'B08', 'CLM'],
                                        bbox=(
                                        25.99965089839723, 63.65600798545179, 26.046183630114623, 63.67816348259773),
                                        spatial_res=0.0001,
                                        time_range=('2020-06-01', '2020-06-30'),
                                        time_period='1D'
                                        )


@unittest.skipUnless(HAS_SH_CREDENTIALS, REQUIRE_SH_CREDENTIALS)
class CubeTest(unittest.TestCase):
    def test_open_cube(self):
        cube = open_cube(cube_config=cube_config)
        self.assertIsInstance(cube, xr.Dataset)
        self.assert_has_zarr_store(cube)

    def test_time_max_none(self):
        cube = open_cube(cube_config=cube_config_t1_none)
        self.assertIsInstance(cube, xr.Dataset)
        self.assertEqual(np.datetime64('today', 'D'),
                         np.datetime64(cube.time.values[-1], 'D'))
        self.assert_has_zarr_store(cube)

    def test_time_none(self):
        cube = open_cube(cube_config=cube_config_t_none)
        self.assertIsInstance(cube, xr.Dataset)
        self.assertEqual(np.datetime64('2021-02-16'),
                         np.datetime64(cube.time.values[-1], 'D'))
        self.assertEqual(np.datetime64('2015-06-27'),
                         np.datetime64(cube.time.values[0], 'D'))
        self.assert_has_zarr_store(cube)

    def assert_has_zarr_store(self, cube):
        if hasattr(cube, 'zarr_store'):
            self.assertIsInstance(cube.zarr_store.get(),
                                  collections.abc.Mapping)


@unittest.skipUnless(HAS_SH_CREDENTIALS, REQUIRE_SH_CREDENTIALS)
class CubeWithCredentialsTest(unittest.TestCase):

    def test_open_cube_with_illegal_kwargs(self):
        with self.assertRaises(ValueError) as cm:
            open_cube(cube_config=cube_config,
                      sentinel_hub=SentinelHub(),
                      api_url="https://creodias.sentinel-hub.com/api/v1/catalog/collections")
        self.assertEqual('unexpected keyword-arguments: api_url', f'{cm.exception}')

    @unittest.skipUnless(HAS_SH_CREDENTIALS, REQUIRE_SH_CREDENTIALS)
    def test_open_cube_with_other_crs(self):
        cube = open_cube(cube_config_with_crs)
        self.assertIsInstance(cube, xr.Dataset)
        self.assertEqual({'time': 160, 'y': 2048, 'x': 2048, 'bnds': 2}, cube.dims)
        self.assertEqual({'x', 'y', 'time', 'time_bnds'}, set(cube.coords))
        self.assertEqual({'B01', 'crs'}, set(cube.data_vars))

    @unittest.skipUnless(HAS_SH_CREDENTIALS, REQUIRE_SH_CREDENTIALS)
    def test_open_cube_LOTL2(self):
        cube = open_cube(cube_config_LOTL2, api_url="https://services-uswest2.sentinel-hub.com")
        self.assertIsInstance(cube, xr.Dataset)
        self.assertEqual({'time': 100, 'lat': 1912, 'lon': 2094, 'bnds': 2}, cube.dims)
        self.assertEqual({'lat', 'lon', 'time', 'time_bnds'}, set(cube.coords))
        self.assertEqual({'B02', 'B03'}, set(cube.data_vars))

    # Commented out because following 2 tests now produce SH errors:
    # xcube_sh.sentinelhub.SentinelHubError:
    #   429 Client Error:
    #   Too Many Requests for url:
    #   https://services.sentinel-hub.com/api/v1/process
    #
    # used to debug xcube-sh issue 60
    # @unittest.skipUnless(HAS_SH_CREDENTIALS, REQUIRE_SH_CREDENTIALS)
    # def test_open_cube_S2L2A_vs_S2L2A_WGS84(self):
    #     cube_wgs84 = open_cube(cube_config_S2L2A_WGS84)
    #     cube = open_cube(cube_config_S2L2A)
    #     self.assertTrue(cube.time.equals(cube_wgs84.time))
    #     self.assertIsInstance(cube, xr.Dataset)
    #     cube = cube.dropna(dim="time")
    #     cube_wgs84 = cube_wgs84.dropna(dim="time")
    #     self.assertTrue(cube.time.equals(cube_wgs84.time))
    #     self.assertEqual(12, len(cube.time))
    #     self.assertEqual({'lat', 'lon', 'time', 'time_bnds'}, set(cube_wgs84.coords))
    #     self.assertEqual({'x', 'y', 'time', 'time_bnds'}, set(cube.coords))
    #     self.assertEqual({'B03', 'B08', 'CLM', 'crs'}, set(cube.data_vars))
    #
    # # used to debug xcube-sh issue 60
    # @unittest.skipUnless(HAS_SH_CREDENTIALS, REQUIRE_SH_CREDENTIALS)
    # def test_open_cube_S2L2A_1D(self):
    #     cube = open_cube(cube_config_S2L2A_1D)
    #     cube_wgs84 = open_cube(cube_config_S2L2A_WGS84_1D)
    #     self.assertIsInstance(cube, xr.Dataset)
    #     self.assertIsInstance(cube_wgs84, xr.Dataset)
    #     cube = cube.dropna(dim="time")
    #     cube_wgs84 = cube_wgs84.dropna(dim="time")
    #     self.assertTrue(cube.time.equals(cube_wgs84.time))
    #     self.assertEqual({'lat', 'lon', 'time', 'time_bnds'}, set(cube_wgs84.coords))
    #     self.assertEqual({'x', 'y', 'time', 'time_bnds'}, set(cube.coords))
    #     self.assertEqual({'B03', 'B08', 'CLM', 'crs'}, set(cube.data_vars))
