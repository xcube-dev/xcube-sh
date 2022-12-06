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

import unittest

import numpy as np

from xcube_sh.config import CubeConfig
from xcube_sh.cube import open_cube
from xcube_sh.sentinelhub import SentinelHub


@unittest.skip(reason="should be fixed")
class ReproduceIssue89Test(unittest.TestCase):
    """
    This test reproduces issue #89.
    """

    def test_reproduce_issue_89(self):
        sentinel_hub = SentinelHub(
            api_url='https://services-uswest2.sentinel-hub.com'
        )

        cube_config = CubeConfig(
            dataset_name="LOTL2",
            band_names=["B03", "B04", "B05", "BQA"],
            crs='EPSG:4326',
            bbox=(12.49, 41.88, 12.53, 41.92),
            spatial_res=0.0001,
            time_range=('2017-07-01', '2017-07-16'),
            time_tolerance="30m",
        )

        cube = open_cube(cube_config, sentinel_hub=sentinel_hub)

        expected_data_types = [np.float32, np.float32, np.float32, np.uint16]
        for i, b in enumerate(["B03", "B04", "B05", "BQA"]):
            band = cube[b]
            self.assertEqual(expected_data_types[i], band.dtype, b)
