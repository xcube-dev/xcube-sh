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
from typing import Tuple

from xcube_sh.sentinelhub import SentinelHubMetadata


class SentinelHubMetadataTest(unittest.TestCase):
    def test_dataset_names(self):
        md = SentinelHubMetadata()

        self.assertEqual({'S1GRD', 'S2L1C', 'S2L2A', 'S3OLCI', 'S3SLSTR', 'S5PL2',
                          'LOTL1', 'LOTL2', 'L8L1C',
                          'DEM', 'MODIS', 'CUSTOM'},
                         set(md.dataset_names))

    def test_dataset_band(self):
        md = SentinelHubMetadata()

        self.assertEqual({'bandwidth': 92.5,
                          'bandwidth_a': 91,
                          'bandwidth_b': 94,
                          'fill_value': 0.0,
                          'resolution': 20,
                          'sample_type': 'FLOAT32',
                          'units': 'reflectance',
                          'wavelength': 1612.05,
                          'wavelength_a': 1613.7,
                          'wavelength_b': 1610.4},
                         md.dataset_band('S2L1C', 'B11'))

        self.assertEqual({'bandwidth': 66.0,
                          'bandwidth_a': 66,
                          'bandwidth_b': 66,
                          'fill_value': 0.0,
                          'resolution': 10,
                          'sample_type': 'FLOAT32',
                          'units': 'reflectance',
                          'wavelength': 492.25,
                          'wavelength_a': 492.4,
                          'wavelength_b': 492.1},
                         md.dataset_band('S2L2A', 'B02'))

        self.assertEqual(['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B10', 'B11', 'BQA',
                          'QA_RADSAT', 'VAA', 'VZA', 'SAA', 'SZA'],
                         md.dataset_band_names('LOTL1'))

    def test_wavelengths_numeric(self):
        self.assertEqual((0, 0), self._assert_all_wavelengths_numeric('S1GRD'))
        self.assertEqual((13, 13), self._assert_all_wavelengths_numeric('S2L1C'))
        self.assertEqual((13, 13), self._assert_all_wavelengths_numeric('S2L2A'))
        self.assertEqual((21, 21), self._assert_all_wavelengths_numeric('S3OLCI'))
        self.assertEqual((11, 11), self._assert_all_wavelengths_numeric('S3SLSTR'))
        self.assertEqual((0, 0), self._assert_all_wavelengths_numeric('S5PL2'))
        self.assertEqual((12, 0), self._assert_all_wavelengths_numeric('L8L1C'))
        self.assertEqual((17, 0), self._assert_all_wavelengths_numeric('LOTL1'))
        self.assertEqual((19, 0), self._assert_all_wavelengths_numeric('LOTL2'))
        self.assertEqual((7, 7), self._assert_all_wavelengths_numeric('MODIS'))

    def _assert_all_wavelengths_numeric(self, ds_name: str) -> Tuple[int, int]:
        return (self._assert_all_band_fields_numeric(ds_name, 'wavelength'),
                self._assert_all_band_fields_numeric(ds_name, 'bandwidth'))

    def _assert_all_band_fields_numeric(self, ds_name: str, field_name: str) -> int:
        md = SentinelHubMetadata()
        n = 0
        for b_name in md.dataset_band_names(ds_name):
            band = md.dataset_band(ds_name, b_name)
            if field_name in band:
                self.assertIsInstance(band[field_name], (int, float))
                n += 1
        return n
