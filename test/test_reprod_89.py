# Copyright © 2022-2024 by the xcube development team and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

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
        sentinel_hub = SentinelHub(api_url="https://services-uswest2.sentinel-hub.com")

        cube_config = CubeConfig(
            dataset_name="LOTL2",
            band_names=["B03", "B04", "B05", "BQA"],
            crs="EPSG:4326",
            bbox=(12.49, 41.88, 12.53, 41.92),
            spatial_res=0.0001,
            time_range=("2017-07-01", "2017-07-16"),
            time_tolerance="30m",
        )

        cube = open_cube(cube_config, sentinel_hub=sentinel_hub)

        expected_data_types = [np.float32, np.float32, np.float32, np.uint16]
        for i, b in enumerate(["B03", "B04", "B05", "BQA"]):
            band = cube[b]
            self.assertEqual(expected_data_types[i], band.dtype, b)
