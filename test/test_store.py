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

from test.test_sentinelhub import HAS_SH_CREDENTIALS
from test.test_sentinelhub import REQUIRE_SH_CREDENTIALS
from xcube.core.store import DatasetDescriptor
from xcube.core.store import VariableDescriptor
from xcube.core.store import find_data_opener_extensions
from xcube.core.store import find_data_store_extensions
from xcube.core.store import new_data_opener
from xcube.core.store import new_data_store
from xcube.util.jsonschema import JsonObjectSchema
from xcube_sh.constants import SH_DATA_OPENER_ID
from xcube_sh.constants import SH_DATA_STORE_ID
from xcube_sh.store import SentinelHubDataOpener
from xcube_sh.store import SentinelHubDataStore


class SentinelHubDataStorePluginTest(unittest.TestCase):
    def test_find_data_store_extensions(self):
        extensions = find_data_store_extensions()
        actual_ext = set(ext.name for ext in extensions)
        self.assertIn(SH_DATA_STORE_ID, actual_ext)

    def test_find_data_opener_extensions(self):
        extensions = find_data_opener_extensions()
        actual_ext = set(ext.name for ext in extensions)
        self.assertIn(SH_DATA_OPENER_ID, actual_ext)


@unittest.skipUnless(HAS_SH_CREDENTIALS, REQUIRE_SH_CREDENTIALS)
class SentinelHubDataOpenerTest(unittest.TestCase):
    def test_new_data_opener(self):
        opener = new_data_opener(SH_DATA_OPENER_ID)
        self.assertIsInstance(opener, SentinelHubDataOpener)

    def test_data_opener_params_schema(self):
        opener = new_data_opener(SH_DATA_OPENER_ID)
        schema = opener.get_open_data_params_schema('S2L2A')
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertEqual('object', schema.type)
        self.assertEqual({'time_range', 'spatial_res', 'bbox'}, schema.required)
        self.assertIn('time_range', schema.properties)
        self.assertIn('time_period', schema.properties)
        self.assertIn('spatial_res', schema.properties)
        self.assertIn('bbox', schema.properties)
        self.assertIn('crs', schema.properties)


@unittest.skipUnless(HAS_SH_CREDENTIALS, REQUIRE_SH_CREDENTIALS)
class SentinelHubDataStoreTest(unittest.TestCase):

    def test_new_data_store(self):
        store = new_data_store(SH_DATA_STORE_ID)
        self.assertIsInstance(store, SentinelHubDataStore)

    def test_get_type_specifiers(self):
        store = new_data_store(SH_DATA_STORE_ID)
        self.assertEqual(('dataset[cube]',), store.get_type_specifiers())
        self.assertEqual(('dataset[cube]',), store.get_type_specifiers_for_data('S2L2A'))

    def test_get_data_opener_ids(self):
        store = new_data_store(SH_DATA_STORE_ID)
        self.assertEqual(('dataset[cube]:zarr:sentinelhub',), store.get_data_opener_ids())
        self.assertEqual(('dataset[cube]:zarr:sentinelhub',), store.get_data_opener_ids(type_specifier='dataset'))
        self.assertEqual((), store.get_data_opener_ids(type_specifier='geodataframe'))

    def test_get_data_ids(self):
        store = new_data_store(SH_DATA_STORE_ID)
        expected_set = {('S1GRD', 'Sentinel 1 GRD'),
                        ('S2L1C', 'Sentinel 2 L1C'),
                        ('S2L2A', 'Sentinel 2 L2A'),
                        ('DEM', 'Digital Elevation Model'),}
        self.assertEqual(expected_set, set(store.get_data_ids()))
        self.assertEqual(expected_set, set(store.get_data_ids(type_specifier='dataset')))
        self.assertEqual(expected_set, set(store.get_data_ids(type_specifier='dataset[cube]')))
        self.assertEqual(set(), set(store.get_data_ids(type_specifier='geodataframe')))

    def test_get_open_data_params_schema(self):
        store = new_data_store(SH_DATA_STORE_ID)
        schema = store.get_open_data_params_schema('S2L2A')
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertEqual('object', schema.type)
        self.assertEqual({'time_range', 'spatial_res', 'bbox'}, schema.required)
        self.assertIn('bbox', schema.properties)
        self.assertIn('time_range', schema.properties)
        self.assertEqual(
            {
                'type':  ['array', 'null'],
                'items': [{'type': ['string', 'null'], 'format': 'date', 'minDate': '2016-11-01'},
                          {'type': ['string', 'null'], 'format': 'date', 'minDate': '2016-11-01'}],
            },
            schema.properties['time_range'].to_dict())
        self.assertIn('time_period', schema.properties)
        self.assertIn('spatial_res', schema.properties)
        self.assertIn('crs', schema.properties)

    def test_describe_data(self):
        store = new_data_store(SH_DATA_STORE_ID)
        dsd = store.describe_data('S2L1C')
        self.assertIsInstance(dsd, DatasetDescriptor)
        self.assertEqual('S2L1C', dsd.data_id)
        self.assertIsInstance(dsd.data_vars, list)
        for vd in dsd.data_vars:
            self.assertIsInstance(vd, VariableDescriptor)
        self.assertEqual(
            {
                'B01',
                'B02',
                'B03',
                'B04',
                'B05',
                'B06',
                'B07',
                'B08',
                'B8A',
                'B09',
                'B10',
                'B11',
                'B12',
                'CLP',
                'CLM',
                'sunZenithAngles',
                'sunAzimuthAngles',
                'viewZenithMean',
                'viewAzimuthMean',
            }, set(vd.name for vd in dsd.data_vars))
        self.assertEqual(None, dsd.crs)
        self.assertEqual(None, dsd.spatial_res)
        self.assertEqual((-180.0, -56.0, 180.0, 83.0), dsd.bbox)
        self.assertEqual(('2015-11-01', None), dsd.time_range)
        self.assertEqual('1D', dsd.time_period)
