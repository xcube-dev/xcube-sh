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
from typing import Any

from test.psycopg2_mock import connect
from xcube_sh.geodb import get_geo_db_service, LocalGeoDBService, RemoteGeoPostgreSQLService


class GeoDBServiceTest(unittest.TestCase):
    def test_get_geo_db_service(self):
        s = get_geo_db_service(local=True)
        self.assertIsInstance(s, LocalGeoDBService)

        s = get_geo_db_service(local=False)
        self.assertIsInstance(s, RemoteGeoPostgreSQLService)


def get_geodb_service_mock(tgt: Any) -> Any:
    conn = connect(tgt=tgt)
    return RemoteGeoPostgreSQLService(conn=conn, host='test')


class GeoDBLocalServiceTest(unittest.TestCase):

    def setUp(self) -> None:
        self.geo_db = get_geo_db_service(driver='local')

    def test_find_feature(self):
        x = self.geo_db.find_feature(collection_name='germany-sh-lakes', query='S_NAME == "Lago di Garda"')
        self.assertIsNone(x)

        x = self.geo_db.find_feature(collection_name='germany-sh-lakes', query='S_NAME == "Selenter_See"')
        self.assertIsNotNone(x)

    def test_find_features(self):
        x = self.geo_db.find_features(collection_name='germany-sh-lakes', query='S_NAME == "Lago di Garda"')
        self.assertEqual(0, len(x))

        x = self.geo_db.find_features(collection_name='germany-sh-lakes', query='S_NAME == "Selenter_See"')
        self.assertEqual(1, len(x))

    def test_find_feature_collection_not_exists(self):
        with self.assertRaises(FileNotFoundError) as cm:
            self.geo_db.find_features(collection_name='germany-sh', query='S_NAME == "Lago di Garda"')

        self.assertEqual("Could not find file germany-sh.geojson", str(cm.exception))


class GeoDBremotePsqlServiceTest(unittest.TestCase):

    def setUp(self) -> None:
        # self.geo_db = RemoteGeoPostgreSQLService(conn=conn, host='test')
        self.geo_db = get_geo_db_service(
            driver='pgsql',
            host="db-dcfs-geodb.cbfjgqxk302m.eu-central-1.rds.amazonaws.com",
            user="postgres",
            password="Oeckel6b&z"
        )

    def test_find_feature_none(self):
        tgt = {
            'information_schema': {
                'fetchall': [1],
                'rowcount': 1,
            },
            'json_build_object': {
                'fetchall': [],
                'rowcount': 0,
            }
        }

        geo_db = get_geodb_service_mock(tgt=tgt)
        x = geo_db.find_feature(collection_name='germany-sh-lakes', query="'S_NAME'='Lago di Garda'")
        self.assertIsNone(x)

    def test_find_feature_one(self):
        record = {'type': 'Feature',
                  'properties':
                      {'id': '74',
                       'METADATA_U': 'http://www.wasserblick.net',
                       'RESTRICTED': '1',
                       'TEMPLATE': 'Lwseggeom',
                       'S_NAME': 'Einfelder_See',
                       'EU_CD_LS': 'DE_LS_DESH_0072', },
                  'geometry': {'type': 'Polygon',
                               'coordinates': [[[9.993225, 54.1375089999961, 0],
                                                [9.993311, 54.1376349999961, 0],
                                                [9.993225, 54.1375089999961, 0]]]}}
        tgt = {
            'information_schema': {
                'fetchall': [1],
                'rowcount': 1,
            },
            'json_build_object': {
                'fetchall': [[record]],
                'rowcount': 1,
            }
        }

        geo_db = get_geodb_service_mock(tgt=tgt)

        x = geo_db.find_feature(collection_name='germany-sh-lakes', query="'S_NAME'='Einfelder_See'")
        self.assertIsNotNone(x)
        self.assertDictEqual(record, x)

    def test_new_collection(self):
        schema = {'geometry': 'Polygon',
                  'properties': {'CAT': 'float:16',
                                 'FIPS_CNTRY': 'str',
                                 'CNTRY_NAME': 'str',
                                 'AREA': 'float:15.2',
                                 'POP_CNTRY': 'float:15.2'}}

        self.geo_db.new_collection('test3', schema=schema)

    def test_drop_collection(self):
        tgt = {
            'information_schema': {
                'fetchall': [False],
                'rowcount': 1,
            },
        }

        geo_db = get_geodb_service_mock(tgt=tgt)

        with self.assertRaises(ValueError) as cm:
            geo_db.drop_collection('test2')

        self.assertEqual("Collection test2 does not exist", str(cm.exception))

    def test_add_features(self):
        record = {'type': 'Feature',
                  'properties':
                      {'id': '74',
                       'METADATA_U': 'http://www.wasserblick.net',
                       'RESTRICTED': '1',
                       'TEMPLATE': 'Lwseggeom',
                       'S_NAME': 'Einfelder_See',
                       'EU_CD_LS': 'DE_LS_DESH_0072', },
                  'geometry': {'type': 'Polygon',
                               'coordinates': [[[9.993225, 54.1375089999961, 0],
                                                [9.993311, 54.1376349999961, 0],
                                                [9.993225, 54.1375089999961, 0]]]}}
        tgt = {
            'INSERT': {

            },
        }

        geo_db = get_geodb_service_mock(tgt=tgt)
        geo_db.add_features(collection_name='test', features=[record])

    def test_find_feature_collection_not_exists(self):
        tgt = {
            'information_schema': {
                'fetchall': [False],
                'rowcount': 1,
            },
        }

        geo_db = get_geodb_service_mock(tgt=tgt)

        with self.assertRaises(ValueError) as cm:
            geo_db.find_features(collection_name='germany-sh', query='S_NAME == "Lago di Garda"')

        self.assertEqual("Collection germany-sh not found", str(cm.exception))
