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
from unittest import mock

from geopandas import GeoDataFrame

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


# noinspection PyUnusedLocal
def _mock_get_collections(self):
    return ["geodb_test"]


# noinspection PyUnusedLocal
def _mock_collection_exists_true(self, collection_name):
    return True


# noinspection PyUnusedLocal
def _mock_collection_exists_false(self, collection_name):
    return False


RemoteGeoPostgreSQLService._get_collections = _mock_get_collections


class GeoDBremotePsqlServiceTest(unittest.TestCase):

    @mock.patch('psycopg2.connect')
    def test_find_feature_none(self, mock_connect):
        expected = []
        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = expected

        RemoteGeoPostgreSQLService._collection_exists = _mock_collection_exists_true
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
    def test_find_feature_bbox(self, mock_connect):
        mock_con = mock_connect.return_value
        geo_db: RemoteGeoPostgreSQLService = RemoteGeoPostgreSQLService(conn=mock_con, host='test')

        geo_db.find_features(collection_name='test', query="'S_NAME'='Einfelder_See'",
                             bbox=[1, 1, 1, 1], bbox_mode='contains', fmt='geojson')
        self.assertIsNotNone(geo_db.sql)
        expected = ("SELECT  json_build_object(\n"
                    "        'type', 'Feature',\n"
                    "	    'properties', properties::json,\n"
                    "        'geometry', ST_AsGeoJSON(geometry)::json\n"
                    "        )\n"
                    "        FROM \"test\" \n"
                    "        WHERE properties->>'S_NAME'='Einfelder_See' and  "
                    "ST_Contains(' SRID=4326;POLYGON((1 1,1 1,1 1,1 1,1 1))::geometry', geometry) ")

        self.assertEqual(expected, geo_db.sql)

        geo_db.find_features(collection_name='test', query="'S_NAME'='Einfelder_See'",
                             bbox=None, bbox_mode='contains', fmt='geojson')
        self.assertIsNotNone(geo_db.sql)
        expected = ("SELECT  json_build_object(\n"
                    "        'type', 'Feature',\n"
                    "	    'properties', properties::json,\n"
                    "        'geometry', ST_AsGeoJSON(geometry)::json\n"
                    "        )\n"
                    "        FROM \"test\" \n"
                    "        WHERE properties->>'S_NAME'='Einfelder_See' ")
        self.assertEqual(expected, geo_db.sql)

        geo_db.find_features(collection_name='test', query=None, bbox=[1, 1, 1, 1], bbox_mode='contains',
                             fmt='geojson')
        self.assertIsNotNone(geo_db.sql)
        expected = ("SELECT  json_build_object(\n"
                    "        'type', 'Feature',\n"
                    "	    'properties', properties::json,\n"
                    "        'geometry', ST_AsGeoJSON(geometry)::json\n"
                    "        )\n"
                    "        FROM \"test\" \n"
                    "        WHERE  ST_Contains(' SRID=4326;POLYGON((1 1,1 1,1 1,1 1,1 1))::geometry', geometry) ")
        self.assertEqual(expected, geo_db.sql)

        x = geo_db._alter_query(query=None, bbox=None, bbox_mode='contains', fmt='geojson')
        self.assertIsNotNone(x)
        expected = ("SELECT  json_build_object(\n"
                    "        'type', 'Feature',\n"
                    "	    'properties', properties::json,\n"
                    "        'geometry', ST_AsGeoJSON(geometry)::json\n"
                    "        )\n"
                    "        FROM \"test\" \n"
                    "        WHERE  ST_Contains(' SRID=4326;POLYGON((1 1,1 1,1 1,1 1,1 1))::geometry', geometry) ")
        self.assertEqual(expected, geo_db.sql)

    @mock.patch('psycopg2.connect')
    def test_find_features_geopandas(self, mock_connect):
        RemoteGeoPostgreSQLService._collection_exists = _mock_collection_exists_true
        expected = [["141",
                     '4770326',
                     '2019-03-26',
                     '0103000020E610000001000000110000007593188402B51B41B6F3FDD4423FF6405839B4C802B51B412B8716D9'
                     'EC3EF6406F1283C0EBB41B41A8C64B37C53EF640B6F3FDD4E4B41B419A999999A33EF6400E2DB29DCFB41B41EE7C3'
                     'F35B63EF6407F6ABC74C0B41B41EE7C3F35B63EF6407B14AE47BDB41B41AAF1D24D043FF6408B6CE77B64B41B413F355E'
                     'BA8F3FF6402B8716D970B41B41986E1283EC3FF640A4703D0A76B41B4179E92631AE3FF6404260E5D08AB41B4123DBF97'
                     'E923FF6409EEFA7C69CB41B4100000000AC3FF6405839B448B3B41B411D5A643B973FF6408195438BC6B41B41666'
                     '666666C3FF640D122DBF9E3B41B4139B4C876383FF640E9263188F8B41B41333333333D3FF640'
                     '7593188402B51B41B6F3FDD4423FF640',
                     ]]
        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = expected
        mock_cur.description = [["RABA_PID"], ['RABA_ID'], ['D_OD'], ['geometry']]

        geo_db = RemoteGeoPostgreSQLService(conn=mock_con, host='test')

        x = geo_db.find_features(collection_name='test', query="'S_NAME'='Einfelder_See'",
                                 bbox=None, bbox_mode='contains', fmt='geopandas')

        self.assertIsInstance(x, GeoDataFrame)

        expected = ("SELECT  *\n"
                    "        FROM \"test\" \n"
                    "        WHERE 'S_NAME'='Einfelder_See' ")
        self.assertEqual(expected, geo_db.sql)

    @mock.patch('psycopg2.connect')
    def test_new_collection(self, mock_connect):
        schema = {'geometry': 'Polygon',
                  'properties': {'CAT': 'float:16',
                                 'FIPS_CNTRY': 'str',
                                 'CNTRY_NAME': 'str',
                                 'AREA': 'float:15.2',
                                 'POP_CNTRY': 'float:15.2'}}

        mock_con = mock_connect.return_value
        RemoteGeoPostgreSQLService._collection_exists = _mock_collection_exists_false

        geo_db = RemoteGeoPostgreSQLService(conn=mock_con, host='test')

        result = geo_db.new_collection('test3', schema=schema)
        self.assertEqual("Collection created", result)

    @mock.patch('psycopg2.connect')
    def test_drop_collection(self, mock_connect):
        expected = (False,)
        mock_con = mock_connect.return_value
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = expected

        geo_db = RemoteGeoPostgreSQLService(conn=mock_con, host='test')

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
        mock_cur = mock_con.cursor.return_value
        mock_cur.fetchall.return_value = (False,)

        geo_db = RemoteGeoPostgreSQLService(conn=mock_con, host='test')

        with self.assertRaises(ValueError) as cm:
            geo_db.find_features(collection_name='germany-sh', query='S_NAME == "Lago di Garda"', fmt='geojson')

        self.assertEqual("Collection germany-sh not found", str(cm.exception))
