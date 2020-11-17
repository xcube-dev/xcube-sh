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

import unittest

import pandas as pd

from xcube_sh.config import CubeConfig


class CubeConfigTest(unittest.TestCase):

    def test_adjust_sizes(self):
        spatial_res = 0.00018
        common_kwargs = dict(dataset_name='S2L2A',
                             band_names=('B01', 'B02', 'B03'),
                             spatial_res=spatial_res,
                             tile_size=(512, 512),
                             time_range=('2019-01-01', '2019-01-02'))

        # size will be smaller than chunk sizes
        config = CubeConfig(bbox=(10.11, 54.17, 10.14, 54.19), **common_kwargs)
        w, h = config.size
        x1, y1, x2, y2 = config.bbox
        self.assertEqual((167, 111), (w, h))
        self.assertEqual((167, 111), config.tile_size)
        self.assertEqual((1, 1), config.num_tiles)
        self.assertAlmostEqual(10.11, x1)
        self.assertAlmostEqual(10.14006, x2, places=4)
        self.assertAlmostEqual(54.17, y1)
        self.assertAlmostEqual(54.18998, y2, places=4)
        self.assertEqual(w, round((x2 - x1) / spatial_res))
        self.assertEqual(h, round((y2 - y1) / spatial_res))

        # size will be smaller than 2x chunk sizes
        config = CubeConfig(bbox=(10.11, 54.17, 10.2025, 54.3), **common_kwargs)
        w, h = config.size
        x1, y1, x2, y2 = config.bbox
        self.assertEqual((514, 722), (w, h))
        self.assertEqual((514, 722), config.tile_size)
        self.assertEqual((1, 1), config.num_tiles)
        self.assertAlmostEqual(10.11, x1)
        self.assertAlmostEqual(10.20252, x2, places=4)
        self.assertAlmostEqual(54.17, y1)
        self.assertAlmostEqual(54.29996, y2, places=4)
        self.assertEqual(w, round((x2 - x1) / spatial_res))
        self.assertEqual(h, round((y2 - y1) / spatial_res))

        # size will be larger than or equal 2x chunk sizes
        config = CubeConfig(bbox=(10.11, 54.17, 10.5, 54.5), **common_kwargs)
        w, h = config.size
        x1, y1, x2, y2 = config.bbox
        self.assertEqual((2560, 2048), (w, h))
        self.assertEqual((512, 512), config.tile_size)
        self.assertEqual((5, 4), config.num_tiles)
        self.assertAlmostEqual(10.11, x1)
        self.assertAlmostEqual(10.57080, x2, places=4)
        self.assertAlmostEqual(54.17, y1)
        self.assertAlmostEqual(54.53864, y2, places=4)
        self.assertEqual(w, round((x2 - x1) / spatial_res))
        self.assertEqual(h, round((y2 - y1) / spatial_res))

    def test_time_deltas(self):
        config = CubeConfig.from_dict(dict(dataset_name='S2L2A',
                                           band_names=('B01', 'B02', 'B03'),
                                           bbox=(10.11, 54.17, 10.14, 54.19),
                                           spatial_res=0.00001,
                                           time_range=('2019-01-01', '2019-01-02')))
        self.assertEqual(None, config.time_period)
        self.assertEqual(pd.Timedelta('0 days 00:10:00'), config.time_tolerance)

        config = CubeConfig.from_dict(dict(dataset_name='S2L2A',
                                           band_names=('B01', 'B02', 'B03'),
                                           bbox=(10.11, 54.17, 10.14, 54.19),
                                           spatial_res=0.00001,
                                           time_period='8D',
                                           time_range=('2019-01-01', '2019-01-02')))
        self.assertEqual(pd.Timedelta('8 days 00:00:00'), config.time_period)
        self.assertEqual(None, config.time_tolerance)

        config = CubeConfig.from_dict(dict(dataset_name='S2L2A',
                                           band_names=('B01', 'B02', 'B03'),
                                           bbox=(10.11, 54.17, 10.14, 54.19),
                                           spatial_res=0.00001,
                                           time_tolerance='1H',
                                           time_range=('2019-01-01', '2019-01-02')))
        self.assertEqual(None, config.time_period)
        self.assertEqual(pd.Timedelta('0 days 01:00:00'), config.time_tolerance)

        config = CubeConfig.from_dict(dict(dataset_name='S2L2A',
                                           band_names=('B01', 'B02', 'B03'),
                                           bbox=(10.11, 54.17, 10.14, 54.19),
                                           spatial_res=0.00001,
                                           time_period='8D',
                                           time_tolerance='1H',
                                           time_range=('2019-01-01', '2019-01-02')))
        self.assertEqual(pd.Timedelta('8 days 00:00:00'), config.time_period)
        self.assertEqual(pd.Timedelta('0 days 01:00:00'), config.time_tolerance)

    def test_from_and_to_dict(self):
        config = CubeConfig.from_dict(dict(dataset_name='S2L2A',
                                           band_names=('B01', 'B02', 'B03'),
                                           bbox=(10.11, 54.17, 10.14, 54.19),
                                           spatial_res=0.00001,
                                           tile_size=(512, 512),
                                           time_range=('2019-01-01', '2019-01-02')))
        self.assertEqual({'band_names': ('B01', 'B02', 'B03'),
                          'band_sample_types': None,
                          'band_units': None,
                          'collection_id': None,
                          'crs': 'http://www.opengis.net/def/crs/EPSG/0/4326',
                          'dataset_name': 'S2L2A',
                          'four_d': False,
                          'bbox': (10.11, 54.17, 10.14072, 54.19048),
                          'spatial_res': 1e-05,
                          'tile_size': (512, 512),
                          'time_period': None,
                          'time_range': ('2019-01-01T00:00:00+00:00', '2019-01-02T00:00:00+00:00'),
                          'time_tolerance': '0 days 00:10:00'
                          },
                         config.as_dict())

    def test_from_dict_invalids(self):
        with self.assertRaises(ValueError) as cm:
            CubeConfig.from_dict(dict(dataset_name='S2L2A',
                                      band_names=('B01', 'B02', 'B03'),
                                      bbox=(10.11, 54.17, 10.14, 54.19),
                                      special_res=0.00001,
                                      tile_size=(512, 512),
                                      time_range=('2019-01-01', '2019-01-02')))
        self.assertEqual("Found invalid parameter 'special_res' in cube configuration",
                         f'{cm.exception}')

        with self.assertRaises(ValueError) as cm:
            CubeConfig.from_dict(dict(dataset_name='S2L2A',
                                      band_names=('B01', 'B02', 'B03'),
                                      geometrix=(10.11, 54.17, 10.14, 54.19),
                                      special_res=0.00001,
                                      tile_size=(512, 512),
                                      time_range=('2019-01-01', '2019-01-02')))
        self.assertEqual("Found invalid parameters in cube configuration: 'geometrix', 'special_res'",
                         f'{cm.exception}')

    def test_band_names_may_be_null(self):
        config = CubeConfig(dataset_name='S2L2A',
                            band_names=None,
                            bbox=(10.11, 54.17, 10.14, 54.19),
                            spatial_res=0.00001,
                            time_range=('2019-01-01', '2019-01-02'))
        self.assertEqual(None, config.band_names)

    def test_deprecated_geometry_still_works(self):
        config = CubeConfig(dataset_name='S2L2A',
                            geometry=(10.11, 54.17, 10.14, 54.19),
                            spatial_res=0.00001,
                            time_range=('2019-01-01', '2019-01-02'))
        self.assertEqual(config.geometry, config.bbox)
