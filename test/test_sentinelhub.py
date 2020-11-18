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

import json
import os
import os.path
import pickle
import shutil
import time
import unittest
from typing import Any, Sequence, Dict

import numpy as np
import zarr

from xcube_sh.sentinelhub import SentinelHub, SerializableOAuth2Session

HAS_SH_CREDENTIALS = 'SH_CLIENT_ID' in os.environ and 'SH_CLIENT_SECRET' in os.environ
REQUIRE_SH_CREDENTIALS = 'requires SH credentials'

THIS_DIR = os.path.dirname(__file__)
REQUEST_SINGLE_JSON = os.path.join(THIS_DIR, 'request-single.json')
REQUEST_SINGLE_BYOD_JSON = os.path.join(THIS_DIR, 'request-single-byod.json')
REQUEST_MULTI_JSON = os.path.join(THIS_DIR, 'request-multi.json')
REQUEST_MULTI_BYOD_JSON = os.path.join(THIS_DIR, 'request-multi-byod.json')


@unittest.skipUnless(HAS_SH_CREDENTIALS, REQUIRE_SH_CREDENTIALS)
class SentinelHubCatalogCollectionsTest(unittest.TestCase):
    def test_it(self):
        sentinel_hub = SentinelHub(api_url='https://creodias.sentinel-hub.com')
        # sentinel_hub = SentinelHub(api_url='https://services-uswest2.sentinel-hub.com')
        # sentinel_hub = SentinelHub()
        collections = sentinel_hub.collections()
        self.assertIsInstance(collections, list)
        self.assertTrue(len(collections) >= 1)
        sentinel_hub.close()

        # with open('collections.json', 'w') as fp:
        #     json.dump(collections, fp, indent=2)


@unittest.skipUnless(HAS_SH_CREDENTIALS, REQUIRE_SH_CREDENTIALS)
class SentinelHubCatalogSearchTest(unittest.TestCase):

    def test_get_features(self):
        features = SentinelHub().get_features(collection_name='sentinel-1-grd',
                                              bbox=(13, 45, 14, 46),
                                              time_range=('2019-12-10T00:00:00Z', '2019-12-11T00:00:00Z'))
        # print(json.dumps(features, indent=2))
        self.assertEqual(8, len(features))
        for feature in features:
            self.assertIn('properties', feature)
            properties = feature['properties']
            self.assertIn('datetime', properties)


class SentinelHubCatalogFeaturesTest(unittest.TestCase):
    def test_features_to_time_ranges(self):
        properties = [{'datetime': '2019-09-17T10:35:42Z'}, {'datetime': '2019-09-17T10:35:46Z'},
                      {'datetime': '2019-10-09T10:25:46Z'}, {'datetime': '2019-10-10T10:45:38Z'},
                      {'datetime': '2019-09-19T10:25:44Z'}, {'datetime': '2019-09-20T10:45:35Z'},
                      {'datetime': '2019-09-20T10:45:43Z'}, {'datetime': '2019-09-22T10:35:42Z'},
                      {'datetime': '2019-09-27T10:35:44Z'}, {'datetime': '2019-09-27T10:35:48Z'},
                      {'datetime': '2019-10-02T10:35:47Z'}, {'datetime': '2019-10-04T10:25:47Z'},
                      {'datetime': '2019-10-05T10:45:36Z'}, {'datetime': '2019-10-05T10:45:44Z'},
                      {'datetime': '2019-10-07T10:35:45Z'}, {'datetime': '2019-10-07T10:35:49Z'},
                      {'datetime': '2019-09-29T10:25:46Z'}, {'datetime': '2019-09-30T10:45:37Z'},
                      {'datetime': '2019-09-25T10:45:35Z'}, {'datetime': '2019-09-25T10:45:43Z'},
                      {'datetime': '2019-09-30T10:45:45Z'}, {'datetime': '2019-10-02T10:35:43Z'},
                      {'datetime': '2019-10-10T10:45:46Z'}, {'datetime': '2019-10-12T10:35:44Z'},
                      {'datetime': '2019-09-22T10:35:46Z'}, {'datetime': '2019-09-24T10:25:46Z'},
                      {'datetime': '2019-10-12T10:35:48Z'}, {'datetime': '2019-10-14T10:25:48Z'},
                      {'datetime': '2019-10-15T10:45:36Z'}, {'datetime': '2019-10-15T10:45:44Z'},
                      {'datetime': '2019-10-17T10:35:46Z'}, {'datetime': '2019-10-17T10:35:50Z'}, ]
        features = [dict(properties=p) for p in properties]
        time_ranges = SentinelHub.features_to_time_ranges(features)
        self.assertEqual([('2019-09-17T10:35:42+00:00', '2019-09-17T10:35:46+00:00'),
                          ('2019-09-19T10:25:44+00:00', '2019-09-19T10:25:44+00:00'),
                          ('2019-09-20T10:45:35+00:00', '2019-09-20T10:45:43+00:00'),
                          ('2019-09-22T10:35:42+00:00', '2019-09-22T10:35:46+00:00'),
                          ('2019-09-24T10:25:46+00:00', '2019-09-24T10:25:46+00:00'),
                          ('2019-09-25T10:45:35+00:00', '2019-09-25T10:45:43+00:00'),
                          ('2019-09-27T10:35:44+00:00', '2019-09-27T10:35:48+00:00'),
                          ('2019-09-29T10:25:46+00:00', '2019-09-29T10:25:46+00:00'),
                          ('2019-09-30T10:45:37+00:00', '2019-09-30T10:45:45+00:00'),
                          ('2019-10-02T10:35:43+00:00', '2019-10-02T10:35:47+00:00'),
                          ('2019-10-04T10:25:47+00:00', '2019-10-04T10:25:47+00:00'),
                          ('2019-10-05T10:45:36+00:00', '2019-10-05T10:45:44+00:00'),
                          ('2019-10-07T10:35:45+00:00', '2019-10-07T10:35:49+00:00'),
                          ('2019-10-09T10:25:46+00:00', '2019-10-09T10:25:46+00:00'),
                          ('2019-10-10T10:45:38+00:00', '2019-10-10T10:45:46+00:00'),
                          ('2019-10-12T10:35:44+00:00', '2019-10-12T10:35:48+00:00'),
                          ('2019-10-14T10:25:48+00:00', '2019-10-14T10:25:48+00:00'),
                          ('2019-10-15T10:45:36+00:00', '2019-10-15T10:45:44+00:00'),
                          ('2019-10-17T10:35:46+00:00', '2019-10-17T10:35:50+00:00')],
                         [(tr[0].isoformat(), tr[1].isoformat()) for tr in time_ranges])


@unittest.skipUnless(HAS_SH_CREDENTIALS, REQUIRE_SH_CREDENTIALS)
class SentinelHubGetDataTest(unittest.TestCase):
    OUTPUTS_DIR = os.path.normpath(os.path.join(THIS_DIR, '..', 'test-outputs'))
    RESPONSE_SINGLE_ZARR = os.path.join(OUTPUTS_DIR, 'response-single.zarr')
    RESPONSE_MULTI_ZARR = os.path.join(OUTPUTS_DIR, 'response-multi.zarr')
    RESPONSE_SINGLE_TIF = os.path.join(OUTPUTS_DIR, 'response-single.tif')
    RESPONSE_MULTI_TAR = os.path.join(OUTPUTS_DIR, 'response-multi.tar')

    @classmethod
    def setUpClass(cls) -> None:
        cls._clear_outputs()
        os.mkdir(cls.OUTPUTS_DIR)

    @classmethod
    def _clear_outputs(cls) -> None:
        # noinspection PyUnusedLocal
        def handle_error(func, path, exc_info):
            print(f'error: failed to rmtree {path}')

        shutil.rmtree(cls.OUTPUTS_DIR, ignore_errors=True, onerror=handle_error)

    def test_get_data_single_binary(self):
        with open(REQUEST_SINGLE_JSON, 'r') as fp:
            request = json.load(fp)

        sentinel_hub = SentinelHub()

        t1 = time.perf_counter()
        response = sentinel_hub.get_data(request, mime_type='application/octet-stream')
        t2 = time.perf_counter()
        print(f"test_get_data_single_binary: took {t2 - t1} secs")

        _write_zarr_array(self.RESPONSE_SINGLE_ZARR, response.content, 0, (512, 512, 1), '<f4')

        sentinel_hub.close()

        zarr_array = zarr.open_array(self.RESPONSE_SINGLE_ZARR)
        self.assertEqual((1, 512, 512, 1), zarr_array.shape)
        self.assertEqual((1, 512, 512, 1), zarr_array.chunks)
        np_array = np.array(zarr_array).astype(np.float32)
        self.assertEqual(np.float32, np_array.dtype)
        np.testing.assert_almost_equal(np.array([0.6425, 0.6676,
                                                 0.5922, 0.5822,
                                                 0.5735, 0.4921,
                                                 0.5902, 0.6518,
                                                 0.5825, 0.5321], dtype=np.float32),
                                       np_array[0, 0, 0:10, 0])
        np.testing.assert_almost_equal(np.array([0.8605, 0.8528,
                                                 0.8495, 0.8378,
                                                 0.8143, 0.7959,
                                                 0.7816, 0.7407,
                                                 0.7182, 0.7326], dtype=np.float32),
                                       np_array[0, 511, -10:, 0])

    @unittest.skip('Known to fail, see TODO in code')
    def test_get_data_multi_binary(self):
        with open(REQUEST_MULTI_JSON, 'r') as fp:
            request = json.load(fp)

        sentinel_hub = SentinelHub()

        # TODO (forman): discuss with Primoz how to effectively do multi-bands request
        t1 = time.perf_counter()
        response = sentinel_hub.get_data(request, mime_type='application/octet-stream')
        t2 = time.perf_counter()
        print(f"test_get_data_multi_binary: took {t2 - t1} secs")

        _write_zarr_array(self.RESPONSE_MULTI_ZARR, response.content, 0, (512, 512, 4), '<f4')

        sentinel_hub.close()

        zarr_array = zarr.open_array(self.RESPONSE_MULTI_ZARR)
        self.assertEqual((1, 512, 512, 4), zarr_array.shape)
        self.assertEqual((1, 512, 512, 4), zarr_array.chunks)
        np_array = np.array(zarr_array).astype(np.float32)
        self.assertEqual(np.float32, np_array.dtype)
        np.testing.assert_almost_equal(np.array([0.6425, 0.6676,
                                                 0.5922, 0.5822,
                                                 0.5735, 0.4921,
                                                 0.5902, 0.6518,
                                                 0.5825, 0.5321], dtype=np.float32),
                                       np_array[0, 0, 0:10, 0])
        np.testing.assert_almost_equal(np.array([0.8605, 0.8528,
                                                 0.8495, 0.8378,
                                                 0.8143, 0.7959,
                                                 0.7816, 0.7407,
                                                 0.7182, 0.7326], dtype=np.float32),
                                       np_array[0, 511, -10:, 0])

    def test_get_data_single(self):
        with open(REQUEST_SINGLE_JSON, 'r') as fp:
            request = json.load(fp)

        sentinel_hub = SentinelHub()

        t1 = time.perf_counter()
        response = sentinel_hub.get_data(request)
        t2 = time.perf_counter()
        print(f"test_get_data_single: took {t2 - t1} secs")

        with open(self.RESPONSE_SINGLE_TIF, 'wb') as fp:
            fp.write(response.content)

        sentinel_hub.close()

    def test_get_data_multi(self):
        with open(REQUEST_MULTI_JSON, 'r') as fp:
            request = json.load(fp)

        sentinel_hub = SentinelHub()

        t1 = time.perf_counter()
        response = sentinel_hub.get_data(request)
        t2 = time.perf_counter()
        print(f"test_get_data_multi: took {t2 - t1} secs")

        with open(self.RESPONSE_MULTI_TAR, 'wb') as fp:
            fp.write(response.content)

        sentinel_hub.close()


class SentinelHubCatalogueTest(unittest.TestCase):

    def test_dataset_names(self):
        expected_dataset_names = ["DEM", "S2L1C", "S2L2A", "CUSTOM", "S1GRD"]
        sentinel_hub = SentinelHub(session=SessionMock({
            'get': {
                'https://services.sentinel-hub.com/configuration/v1/datasets':
                    [{'id': "DEM"}, {'id': "S2L1C"}, {'id': "S2L2A"}, {'id': "CUSTOM"}, {'id': "S1GRD"}]

            }}))
        self.assertEqual(expected_dataset_names,
                         sentinel_hub.dataset_names)

    def test_variable_names(self):
        expected_band_names = ['B01',
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
                               'viewZenithMean',
                               'viewAzimuthMean',
                               'sunZenithAngles',
                               'sunAzimuthAngles']
        sentinel_hub = SentinelHub(session=SessionMock({
            'get': {
                'https://services.sentinel-hub.com/api/v1/process/dataset/S2L2A/bands': {
                    'data': expected_band_names
                }
            }
        }))
        self.assertEqual(expected_band_names, sentinel_hub.band_names('S2L2A'))
        sentinel_hub.close()

    def test_get_features(self):
        properties = [{'datetime': '2019-10-02T10:35:47Z'}, {'datetime': '2019-10-04T10:25:47Z'},
                      {'datetime': '2019-10-05T10:45:36Z'}, {'datetime': '2019-10-05T10:45:44Z'}]
        expected_features = [dict(properties=p) for p in properties]
        sentinel_hub = SentinelHub(session=SessionMock({
            'post': {
                'https://services.sentinel-hub.com/api/v1/catalog/search':
                    dict(type='FeatureCollection', features=expected_features)
            }
        }))
        self.assertEqual(expected_features, sentinel_hub.get_features(collection_name='sentinel-2-l2a',
                                                                      bbox=(12, 53, 13, 54),
                                                                      time_range=('2019-10-02', '2019-10-05')))
        sentinel_hub.close()


@unittest.skipUnless(HAS_SH_CREDENTIALS, REQUIRE_SH_CREDENTIALS)
class SentinelHubTokenInfoTest(unittest.TestCase):

    def test_token_info(self):
        expected_token_info = {
            'name': 'Norman Fomferra',
            'email': 'norman.fomferra@brockmann-consult.de',
            'active': True
        }
        sentinel_hub = SentinelHub(session=SessionMock({
            'get': {
                'https://services.sentinel-hub.com/oauth/tokeninfo':
                    expected_token_info
            }
        }))
        self.assertEqual(expected_token_info, {k: v for k, v in sentinel_hub.token_info.items()
                                               if k in ['name', 'email', 'active']})
        sentinel_hub.close()


class SentinelHubNewRequestTest(unittest.TestCase):

    def test_new_data_request_single(self):
        request = SentinelHub.new_data_request(
            'S2L1C',
            ['B02'],
            (512, 512),
            time_range=("2018-10-01T00:00:00.000Z", "2018-10-10T00:00:00.000Z"),
            bbox=(
                13.822,
                45.850,
                14.559,
                46.291,
            ),
            band_sample_types="FLOAT32",
            band_units="reflectance"
        )

        # with open(os.path.join(REQUEST_SINGLE_JSON, 'w') as fp:
        #    json.dump(request, fp, indent=2)

        with open(REQUEST_SINGLE_JSON, 'r') as fp:
            expected_request = json.load(fp)

        self.assertEqual(expected_request, request)

    def test_new_data_request_multi(self):
        request = SentinelHub.new_data_request(
            'S2L1C',
            ['B02', 'B03', 'B04', 'B08'],
            (512, 512),
            time_range=("2018-10-01T00:00:00.000Z", "2018-10-10T00:00:00.000Z"),
            bbox=(
                13.822,
                45.850,
                14.559,
                46.291,
            ),
            band_sample_types="FLOAT32",
            band_units="reflectance"
        )

        # with open(REQUEST_MULTI_JSON), 'w') as fp:
        #    json.dump(request, fp, indent=2)

        with open(REQUEST_MULTI_JSON, 'r') as fp:
            expected_request = json.load(fp)

        self.assertEqual(expected_request, request)

    def test_new_data_request_single_byod(self):
        request = SentinelHub.new_data_request(
            'CUSTOM',
            ['RED'],
            (512, 305),
            crs="http://www.opengis.net/def/crs/EPSG/0/3857",
            bbox=(
                1545577,
                5761986,
                1705367,
                5857046
            ),
            band_sample_types="UINT8",
            collection_id='1a3ab057-3c51-447c-9f85-27d4b633b3f5'
        )

        # with open(os.path.join(REQUEST_SINGLE_JSON, 'w') as fp:
        #    json.dump(request, fp, indent=2)

        with open(REQUEST_SINGLE_BYOD_JSON, 'r') as fp:
            expected_request = json.load(fp)

        self.assertEqual(expected_request, request)

    def test_new_data_request_multi_byod(self):
        request = SentinelHub.new_data_request(
            'CUSTOM',
            ['RED', 'GREEN', 'BLUE'],
            (512, 305),
            bbox=(
                1545577,
                5761986,
                1705367,
                5857046
            ),
            crs='http://www.opengis.net/def/crs/EPSG/0/3857',
            band_sample_types='UINT8',
            collection_id='1a3ab057-3c51-447c-9f85-27d4b633b3f5'
        )

        # with open(REQUEST_MULTI_JSON), 'w') as fp:
        #    json.dump(request, fp, indent=2)

        with open(REQUEST_MULTI_BYOD_JSON, 'r') as fp:
            expected_request = json.load(fp)

        self.assertEqual(expected_request, request)


class SerializableOAuth2SessionTest(unittest.TestCase):

    def test_pickle(self):
        from oauthlib.oauth2 import BackendApplicationClient
        client = BackendApplicationClient(client_id='sdfvdsv')
        session = SerializableOAuth2Session(client=client)

        self.assertIsNotNone(session._client)
        self.assertIsNotNone(session.adapters)

        actual = pickle.loads(pickle.dumps(session))

        valid_test_attrs = SerializableOAuth2Session._SERIALIZED_ATTRS
        valid_test_attrs.remove('_client')
        valid_test_attrs.remove('adapters')

        actual = actual.__dict__
        actual = dict((k, actual[k]) for k in valid_test_attrs if k in actual)

        expected = session.__dict__
        expected = dict((k, expected[k]) for k in valid_test_attrs if k in expected)

        self.assertEqual(expected, actual)


def _write_zarr_array(dir_path: str,
                      data: Any,
                      time_index: int,
                      data_shape: Sequence[int],
                      data_type: str,
                      attrs: Dict = None):
    height, width, num_vars = data_shape
    shape = 1, height, width, num_vars
    dims = ['time', 'y', 'x', 'var']
    data_file_name = f'{time_index}.0.0.0'
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)
    with open(os.path.join(dir_path, data_file_name), 'wb') as fp:
        fp.write(data)
    with open(os.path.join(dir_path, '.zattrs'), 'w') as fp:
        json.dump(dict(_ARRAY_DIMENSIONS=dims, **(attrs or {})), fp)
    with open(os.path.join(dir_path, '.zarray'), 'w') as fp:
        json.dump({
            "zarr_format": 2,
            "chunks": list(shape),
            "shape": list(shape),
            "compressor": {
                "id": "zlib",
                "level": 8
            },
            "dtype": data_type,
            "fill_value": None,
            "filters": None,
            "order": "C",
        }, fp)


class SessionMock:
    def __init__(self, mapping: Dict):
        self.mapping = mapping

    # noinspection PyUnusedLocal
    def get(self, url, **kwargs):
        return self._response(self.mapping['get'][url])

    # noinspection PyUnusedLocal
    def post(self, url, **kwargs):
        return self._response(self.mapping['post'][url])

    def close(self):
        pass

    @classmethod
    def _response(cls, content_obj):
        return SessionResponseMock(content_obj)


class SessionResponseMock:
    def __init__(self, content_obj):
        self.content_obj = content_obj

    @property
    def ok(self) -> bool:
        return True

    @property
    def content(self):
        return json.dumps(self.content_obj)

    def json(self):
        return self.content_obj
