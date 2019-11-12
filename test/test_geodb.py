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
from unittest import mock
from xcube_sh.geodb import get_geo_db_service, LocalGeoDBService, RemoteGeoPostgreSQLService


class GeoDBServiceTest(unittest.TestCase):
    @mock.patch('psycopg2.connect')
    def test_get_geo_db_service(self, mock_connect):
        s = get_geo_db_service(driver='local')
        self.assertIsInstance(s, LocalGeoDBService)

        mock_con = mock_connect.return_value
        s = get_geo_db_service(driver='psql', host='test', conn=mock_con)
        self.assertIsInstance(s, RemoteGeoPostgreSQLService)


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


def _get_collections(self):
    return ["geodb_test"]


RemoteGeoPostgreSQLService._get_collections = _get_collections


class GeoDBremotePsqlServiceTest(unittest.TestCase):

    @mock.patch('psycopg2.connect')
    def test_find_feature_none(self, mock_connect):
        expected = []
        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = expected

        geo_db = RemoteGeoPostgreSQLService(conn=mock_con, host='test')

        x = geo_db.find_feature(collection_name='test', query="'S_NAME'='Lago di Garda'")
        self.assertIsNone(x)

    @mock.patch('psycopg2.connect')
    def test_find_feature_one(self, mock_connect):
        expected = {'type': 'Feature',
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

        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = [[expected]]

        geo_db = RemoteGeoPostgreSQLService(conn=mock_con, host='test')

        x = geo_db.find_feature(collection_name='test', query="'S_NAME'='Einfelder_See'")
        self.assertIsNotNone(x)
        self.assertDictEqual(expected, x)

    @mock.patch('psycopg2.connect')
    def test_new_collection(self, mock_connect):
        schema = {'geometry': 'Polygon',
                  'properties': {'CAT': 'float:16',
                                 'FIPS_CNTRY': 'str',
                                 'CNTRY_NAME': 'str',
                                 'AREA': 'float:15.2',
                                 'POP_CNTRY': 'float:15.2'}}

        mock_con = mock_connect.return_value

        geo_db = RemoteGeoPostgreSQLService(conn=mock_con, host='test')
        geo_db._collections = ["test"]

        result = geo_db.new_collection('test3', schema=schema)
        self.assertEqual("Collection created", result)

    @mock.patch('psycopg2.connect')
    def test_drop_collection(self, mock_connect):

        mock_con = mock_connect.return_value

        geo_db = RemoteGeoPostgreSQLService(conn=mock_con, host='test')
        geo_db._collections = ["test"]

        with self.assertRaises(ValueError) as cm:
            geo_db.drop_collection('test2')

        self.assertEqual("Collection test2 does not exist", str(cm.exception))

    @mock.patch('psycopg2.connect')
    def test_add_features(self, mock_connect):
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

        mock_con = mock_connect.return_value

        geo_db = RemoteGeoPostgreSQLService(conn=mock_con, host='test')
        geo_db._collections = ["test"]

        result = geo_db.add_features(collection_name='test', features=[record])
        self.assertEqual("Features Added", result)

    @mock.patch('psycopg2.connect')
    def test_find_feature_collection_not_exists(self, mock_connect):
        mock_con = mock_connect.return_value

        geo_db = RemoteGeoPostgreSQLService(conn=mock_con, host='test')
        geo_db._collections = ["test"]

        with self.assertRaises(ValueError) as cm:
            geo_db.find_features(collection_name='germany-sh', query='S_NAME == "Lago di Garda"')

        self.assertEqual("Collection germany-sh not found", str(cm.exception))
