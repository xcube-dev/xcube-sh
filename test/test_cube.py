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


@unittest.skipUnless(HAS_SH_CREDENTIALS, REQUIRE_SH_CREDENTIALS)
class CubeTest(unittest.TestCase):
    def test_open_cube(self):
        cube = open_cube(cube_config=cube_config)
        self.assertIsInstance(cube, xr.Dataset)

    def test_time_max_none(self):
        cube = open_cube(cube_config=cube_config_t1_none)
        self.assertIsInstance(cube, xr.Dataset)
        self.assertEqual(np.datetime64('today', 'D'), np.datetime64(cube.time.values[-1], 'D'))  # self.assertEqual()

    def test_time_none(self):
        cube = open_cube(cube_config=cube_config_t_none)
        self.assertIsInstance(cube, xr.Dataset)
        self.assertEqual(np.datetime64('2021-02-16'), np.datetime64(cube.time.values[-1], 'D'))  # self.assertEqual()
        self.assertEqual(np.datetime64('2015-06-27'), np.datetime64(cube.time.values[0], 'D'))  # self.assertEqual()


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
        self.assertEqual({'time': 320, 'y': 2048, 'x': 2048, 'bnds': 2}, cube.dims)
        self.assertEqual({'x', 'y', 'time', 'time_bnds'}, set(cube.coords))
        self.assertEqual({'B01'}, set(cube.data_vars))
