# Copyright © 2022-2024 by the xcube development team and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import json
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
from xcube.util.jsonschema import JsonSchema
from xcube_sh.constants import CRS_ID_TO_URI
from xcube_sh.constants import SH_DATA_OPENER_ID
from xcube_sh.constants import SH_DATA_STORE_ID
from xcube_sh.constants import SH_CDSE_DATA_STORE_ID
from xcube_sh.store import SentinelHubDataOpener
from xcube_sh.store import SentinelHubDataStore
from xcube_sh.store import SentinelHubCdseDataStore


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
        schema = opener.get_open_data_params_schema("S2L2A")
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertEqual("object", schema.type)
        self.assertEqual({"time_range", "spatial_res", "bbox"}, set(schema.required))
        self.assertIn("time_range", schema.properties)
        self.assertIn("time_period", schema.properties)
        self.assertIn("spatial_res", schema.properties)
        self.assertIn("bbox", schema.properties)
        self.assertIn("crs", schema.properties)
        schema = schema.properties["crs"]
        self.assertIsInstance(schema, JsonSchema)
        self.assertEqual("string", schema.type)
        self.assertEqual(list(CRS_ID_TO_URI.keys()), schema.enum)


@unittest.skipUnless(HAS_SH_CREDENTIALS, REQUIRE_SH_CREDENTIALS)
class SentinelHubDataStoreTest(unittest.TestCase):
    def test_new_data_store(self):
        store = new_data_store(SH_DATA_STORE_ID)
        self.assertIsInstance(store, SentinelHubDataStore)

    def test_get_type_specifiers(self):
        store = new_data_store(SH_DATA_STORE_ID)
        self.assertEqual(("dataset",), store.get_data_types())
        self.assertEqual(("dataset",), store.get_data_types_for_data("S2L2A"))

    def test_get_data_opener_ids(self):
        store = new_data_store(SH_DATA_STORE_ID)
        self.assertEqual(("dataset:zarr:sentinelhub",), store.get_data_opener_ids())
        self.assertEqual(
            ("dataset:zarr:sentinelhub",),
            store.get_data_opener_ids(data_type="dataset"),
        )
        self.assertEqual((), store.get_data_opener_ids(data_type="geodataframe"))

    def test_get_data_ids(self):
        store = new_data_store(SH_DATA_STORE_ID)
        expected_set = {"S1GRD", "S2L1C", "S2L2A", "DEM"}
        self.assertEqual(expected_set, set(store.get_data_ids()))
        self.assertEqual(expected_set, set(store.get_data_ids(data_type="dataset")))
        self.assertEqual(set(), set(store.get_data_ids(data_type="geodataframe")))

    def test_get_data_ids_with_titles(self):
        store = new_data_store(SH_DATA_STORE_ID)
        expected_set = [
            ("DEM", {"title": "Digital Elevation Model"}),
            ("S1GRD", {"title": "Sentinel 1 GRD"}),
            ("S2L1C", {"title": "Sentinel 2 L1C"}),
            ("S2L2A", {"title": "Sentinel 2 L2A"}),
        ]
        self.assertEqual(
            [x[0] for x in expected_set], sorted(list(store.get_data_ids()))
        )
        self.assertEqual(
            expected_set,
            sorted(
                list(store.get_data_ids(data_type="dataset", include_attrs=["title"])),
                key=lambda x: x[0],
            ),
        )
        self.assertEqual(
            [],
            list(store.get_data_ids(data_type="geodataframe", include_attrs=["title"])),
        )

    def test_get_open_data_params_schema(self):
        store = new_data_store(SH_DATA_STORE_ID)
        schema = store.get_open_data_params_schema("S2L2A")
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertEqual("object", schema.type)
        self.assertEqual({"time_range", "spatial_res", "bbox"}, set(schema.required))

        self.assertIn("bbox", schema.properties)
        self.assertIn("spatial_res", schema.properties)
        self.assertIn("crs", schema.properties)
        self.assertIn("upsampling", schema.properties)
        self.assertIn("downsampling", schema.properties)
        self.assertIn("mosaicking_order", schema.properties)

        self.assertIn("time_range", schema.properties)
        self.assertEqual(
            {
                "type": ["array", "null"],
                "items": [
                    {
                        "type": ["string", "null"],
                        "format": "date",
                        "minDate": "2016-11-01",
                    },
                    {
                        "type": ["string", "null"],
                        "format": "date",
                        "minDate": "2016-11-01",
                    },
                ],
            },
            schema.properties["time_range"].to_dict(),
        )
        self.assertIn("time_period", schema.properties)

    def test_describe_data(self):
        store = new_data_store(SH_DATA_STORE_ID)
        dsd = store.describe_data("S2L1C")
        self.assertIsInstance(dsd, DatasetDescriptor)
        self.assertEqual("S2L1C", dsd.data_id)
        self.assertIsInstance(dsd.data_vars, dict)
        for vd in dsd.data_vars.values():
            self.assertIsInstance(vd, VariableDescriptor)
        self.assertEqual(
            {
                "B01",
                "B02",
                "B03",
                "B04",
                "B05",
                "B06",
                "B07",
                "B08",
                "B8A",
                "B09",
                "B10",
                "B11",
                "B12",
                "CLP",
                "CLM",
                "sunZenithAngles",
                "sunAzimuthAngles",
                "viewZenithMean",
                "viewAzimuthMean",
            },
            set(dsd.data_vars.keys()),
        )
        self.assertEqual(None, dsd.crs)
        self.assertEqual(None, dsd.spatial_res)
        self.assertEqual((-180.0, -56.0, 180.0, 83.0), dsd.bbox)
        self.assertEqual(("2015-11-01", None), dsd.time_range)
        self.assertEqual("1D", dsd.time_period)

        d = dsd.to_dict()
        self.assertIsInstance(d, dict)
        # Assert is JSON-serializable
        json.dumps(d)


class SentinelHubCdseDataStoreTest(unittest.TestCase):
    def test_new_data_store(self):
        store = new_data_store(SH_CDSE_DATA_STORE_ID)
        self.assertIsInstance(store, SentinelHubCdseDataStore)
