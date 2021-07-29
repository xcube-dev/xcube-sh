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

import json
import os
import platform
import random
import time
import warnings
from typing import List, Any, Dict, Tuple, Union, Sequence, Callable

import oauthlib.oauth2
import pandas as pd
import pyproj
import requests
import requests_oauthlib

from xcube_sh.constants import CRS_ID_TO_URI
from xcube_sh.constants import DEFAULT_CLIENT_ID
from xcube_sh.constants import DEFAULT_CLIENT_SECRET
from xcube_sh.constants import DEFAULT_CRS
from xcube_sh.constants import DEFAULT_NUM_RETRIES
from xcube_sh.constants import DEFAULT_RETRY_BACKOFF_BASE
from xcube_sh.constants import DEFAULT_RETRY_BACKOFF_MAX
from xcube_sh.constants import DEFAULT_SH_API_URL
from xcube_sh.constants import DEFAULT_SH_OAUTH2_URL
from xcube_sh.constants import SH_CATALOG_FEATURE_LIMIT
from xcube_sh.metadata import SentinelHubMetadata
from xcube_sh.version import version


class SentinelHub:
    """
    Represents the SENTINEL Hub Cloud API.

    :param client_id: SENTINEL Hub client ID
    :param client_secret: SENTINEL Hub client secret
    :param instance_id:  SENTINEL Hub instance ID (deprecated, no longer used)
    :param api_url: Alternative SENTINEL Hub API URL.
    :param oauth2_url: Alternative SENTINEL Hub OAuth2 API URL.
    :param error_policy: "raise" or "warn". If "raise" an exception is raised on failed API requests.
    :param error_handler: An optional function called with the response from a failed API request.
    :param enable_warnings: Allow emitting warnings on failed API requests.
    :param num_retries: Number of retries for failed API requests, e.g. ```50`` times.
    :param retry_backoff_max: Request retry backoff time in milliseconds, e.g. ``100`` milliseconds
    :param retry_backoff_base:  Request retry backoff base. Must be greater than one, e.g. ``1.5``
    :param session: Optional request session object (mostly for testing).
    """

    METADATA = SentinelHubMetadata()

    def __init__(self,
                 client_id: str = None,
                 client_secret: str = None,
                 instance_id: str = None,
                 api_url: str = None,
                 oauth2_url: str = None,
                 enable_warnings: bool = False,
                 error_policy: str = 'fail',
                 error_handler: Callable[[Any], None] = None,
                 num_retries: int = DEFAULT_NUM_RETRIES,
                 retry_backoff_max: int = DEFAULT_RETRY_BACKOFF_MAX,
                 retry_backoff_base: float = DEFAULT_RETRY_BACKOFF_BASE,
                 session: Any = None):
        if instance_id:
            warnings.warn('instance_id has been deprecated, it is no longer used')
        self.api_url = api_url or os.environ.get('SH_API_URL', DEFAULT_SH_API_URL)
        self.oauth2_url = oauth2_url or os.environ.get('SH_OAUTH2_URL', DEFAULT_SH_OAUTH2_URL)
        self.error_policy = error_policy or 'fail'
        self.error_handler = error_handler
        self.enable_warnings = enable_warnings
        self.num_retries = num_retries
        self.retry_backoff_max = retry_backoff_max
        self.retry_backoff_base = retry_backoff_base
        if session is None:
            # Client credentials
            client_id = client_id or DEFAULT_CLIENT_ID
            client_secret = client_secret or DEFAULT_CLIENT_SECRET

            if not client_id or not client_secret:
                raise ValueError('Both client_id and client_secret must be provided.\n'
                                 'Consider setting environment variables SH_CLIENT_ID and SH_CLIENT_SECRET.\n'
                                 'For more information refer to '
                                 'https://docs.sentinel-hub.com/api/latest/#/API/authentication')

            # Create a OAuth2 session
            client = oauthlib.oauth2.BackendApplicationClient(client_id=client_id)
            self.session: SerializableOAuth2Session = SerializableOAuth2Session(client=client)

            # Get OAuth2 token for the session
            self.token = self.session.fetch_token(token_url=self.oauth2_url + '/token',
                                                  client_id=client_id,
                                                  client_secret=client_secret)
            # print(self.token)
            self.client_id = client_id
        else:
            self.session: Any = session
            self.token = None

    def __del__(self):
        self.close()

    def close(self):
        self.session.close()

    @property
    def token_info(self) -> Dict[str, Any]:
        response = self.session.get(self.oauth2_url + '/tokeninfo')
        SentinelHubError.maybe_raise_for_response(response)
        return response.json()

    # noinspection PyMethodMayBeStatic
    @property
    def dataset_names(self) -> List[str]:
        return [item.get('id') for item in self.datasets]

    # noinspection PyMethodMayBeStatic
    @property
    def datasets(self) -> List[Dict[str, str]]:
        """
        See https://docs.sentinel-hub.com/api/latest/reference/#tag/configuration_dataset
        """
        response = self.session.get(self.api_url + '/configuration/v1/datasets')
        SentinelHubError.maybe_raise_for_response(response)
        return response.json()

    def band_names(self, dataset_name: str) -> List[str]:
        response = self.session.get(self.api_url + f'/api/v1/process/dataset/{dataset_name}/bands')
        SentinelHubError.maybe_raise_for_response(response)
        return response.json().get('data', {})

    def collections(self) -> List[Dict[str, Any]]:
        """
        See https://docs.sentinel-hub.com/api/latest/reference/#operation/getCollections
        """
        response = self.session.get(f'{self.api_url}/api/v1/catalog/collections')
        SentinelHubError.maybe_raise_for_response(response)
        return response.json().get('collections', [])

    def get_features(self,
                     collection_name: str,
                     bbox: Tuple[float, float, float, float] = None,
                     crs: str = None,
                     time_range: Tuple[str, str] = None) -> List[Dict[str, Any]]:
        """
        Get geometric intersections of dataset given by *collection_name*
        with optional *bbox* and *time_range*. The result is returned as a list of
        features, whose properties include a "datetime" field.

        :param collection_name: dataset collection name
        :param bbox: bounding box
        :param crs: Name of a coordinate reference system of the coordinates
            given by *bbox*. Ignored if *bbox* is not given.
        :param time_range: time range
        :return: list of features that include a "datetime" field for all intersections.
        """
        max_feature_count = SH_CATALOG_FEATURE_LIMIT

        request = dict(collections=[collection_name],
                       limit=max_feature_count,
                       # Exclude most of the response data, as this is not required (yet)
                       fields=dict(exclude=['geometry', 'bbox', 'assets', 'links'],
                                   include=['properties.datetime']))
        if bbox:
            source_crs = pyproj.crs.CRS.from_string(crs or DEFAULT_CRS)
            if not source_crs.is_geographic:
                x1, y1, x2, y2 = bbox
                transformer = pyproj.Transformer.from_crs(source_crs, 'WGS84')
                (y1, y2), (x1, x2) = transformer.transform((x1, x2), (y1, y2))
                bbox = x1, y1, x2, y2

            request.update(bbox=bbox)

        if time_range:
            t1, t2 = time_range
            request.update(datetime=f'{t1}/{t2}')

        all_features = []
        features_count = max_feature_count
        feature_offset = 0
        while features_count == max_feature_count:
            response = self.session.post(f'{self.api_url}/api/v1/catalog/search',
                                         json=request,
                                         headers=self._get_request_headers('application/json'))

            SentinelHubError.maybe_raise_for_response(response)

            feature_collection = json.loads(response.content)
            if feature_collection.get('type') != 'FeatureCollection' \
                    or not isinstance(feature_collection.get('features'), list):
                raise SentinelHubError(f'Got unexpected result from {response.url}', response=response)

            features = feature_collection['features']
            all_features.extend(features)
            features_count = len(features)
            feature_offset += features_count
            request.update(next=feature_offset)

        return all_features

    @classmethod
    def features_to_time_ranges(cls,
                                features: List[Dict[str, Any]],
                                max_timedelta: Union[str, pd.Timedelta] = '1H') \
            -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        """
        Convert list of features from SH Catalog into list of time ranges whose time deltas are
        not greater than *max_timedelta*.

        :param features: Tile dictionaries as returned by SH WFS
        :param max_timedelta: Maximum time delta for each generated time range
        :return: List time range tuples.
        """
        max_timedelta = pd.to_timedelta(max_timedelta) if isinstance(max_timedelta, str) else max_timedelta
        feature_properties = [feature["properties"] for feature in features]
        timestamps = sorted(set(pd.to_datetime(properties["datetime"], utc=True)
                                for properties in feature_properties))
        num_timestamps = len(timestamps)
        # noinspection PyTypeChecker
        time_ranges: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
        i = 0
        while i < num_timestamps:
            timestamp1 = timestamp2 = timestamps[i]
            while i < num_timestamps:
                timestamp = timestamps[i]
                if timestamp - timestamp1 >= max_timedelta:
                    break
                timestamp2 = timestamp
                i += 1
            time_ranges.append((timestamp1, timestamp2))
        return time_ranges

    def get_data(self, request: Dict, mime_type=None) -> requests.Response:
        outputs = request['output']['responses']
        if not mime_type:
            if len(outputs) > 1:
                mime_type = 'application/tar'
            else:
                mime_type = outputs[0]['format'].get('type', 'image/tiff')

        num_retries = self.num_retries
        retry_backoff_max = self.retry_backoff_max  # ms
        retry_backoff_base = self.retry_backoff_base

        response = None
        for i in range(num_retries):
            response = self.session.post(self.api_url + f'/api/v1/process',
                                         json=request,
                                         headers=self._get_request_headers(mime_type))
            if response.ok:
                # TODO (forman): verify response headers: response_num_components, response_width, ...
                # response_components = int(response.headers.get('SH-Components', '-1'))
                # response_width = int(response.headers.get('SH-Width', '-1'))
                # response_height = int(response.headers.get('SH-Height', '-1'))
                # response_sample_type = response.headers.get('SH-SampleType')
                return response
            else:
                # Retry after 'Retry-After' with exponential backoff
                retry_min = int(response.headers.get('Retry-After', '100'))
                retry_backoff = random.random() * retry_backoff_max
                retry_total = retry_min + retry_backoff
                if self.enable_warnings:
                    retry_message = f'Error {response.status_code}: {response.reason}. ' \
                                    f'Attempt {i + 1} of {num_retries} to retry after ' \
                                    f'{"%.2f" % retry_min} + {"%.2f" % retry_backoff} = {"%.2f" % retry_total} ms...'
                    warnings.warn(retry_message)
                time.sleep(retry_total / 1000.0)
                retry_backoff_max *= retry_backoff_base

        if self.error_handler:
            self.error_handler(response)

        if self.error_policy == 'fail':
            SentinelHubError.maybe_raise_for_response(response)
        elif self.error_policy == 'warn' and self.enable_warnings:
            try:
                SentinelHubError.maybe_raise_for_response(response)
            except SentinelHubError as e:
                warnings.warn(f'Failed to fetch data: {e}')

        # Return failed response (response.ok == False)
        return response

    @classmethod
    def _get_request_headers(cls, mime_type: str):
        return {
            'Accept': mime_type,
            'User-Agent': f'xcube_sh/{version} '
                          f'{platform.python_implementation()}/{platform.python_version()} '
                          f'{platform.system()}/{platform.version()}'
        }

    @classmethod
    def new_data_request(cls,
                         dataset_name: str,
                         band_names: Sequence[str],
                         size: Tuple[int, int],
                         crs: str = None,
                         bbox: Tuple[float, float, float, float] = None,
                         time_range: Tuple[Union[str, pd.Timestamp], Union[str, pd.Timestamp]] = None,
                         upsampling: str = 'BILINEAR',
                         downsampling: str = 'BILINEAR',
                         mosaicking_order: str = 'mostRecent',
                         collection_id: str = None,
                         band_units: Union[str, Sequence[str]] = None,
                         band_sample_types: Union[str, Sequence[str]] = None) -> Dict:

        if bbox is None:
            bbox = [-180., -90., 180., 90.]

        band_units = band_units or None
        if isinstance(band_units, str):
            band_units = [band_units] * len(band_names)

        band_sample_types = band_sample_types or 'FLOAT32'
        if isinstance(band_sample_types, str):
            band_sample_types = [band_sample_types] * len(band_names)

        data_element = {
            "type": dataset_name,
            "processing": {
                "upsampling": upsampling,
                "downsampling": downsampling
            },
        }

        if any([time_range, mosaicking_order, collection_id]):
            data_element["dataFilter"] = dict()
            if time_range:
                time_range_from, time_range_to = time_range
                time_range_element = {}
                if time_range_from:
                    if not isinstance(time_range_from, str):
                        time_range_from = time_range_from.isoformat()
                    time_range_element['from'] = time_range_from
                if time_range_to:
                    if not isinstance(time_range_to, str):
                        time_range_to = time_range_to.isoformat()
                    time_range_element['to'] = time_range_to
                data_element["dataFilter"].update(timeRange=time_range_element)
            if mosaicking_order and time_range:
                data_element["dataFilter"].update(mosaickingOrder=mosaicking_order)
            if collection_id:
                data_element["dataFilter"].update(collectionId=collection_id)

        input_element = {
            "bounds": {
                "bbox": bbox,
                "properties": {
                    "crs": crs or CRS_ID_TO_URI[DEFAULT_CRS]
                }
            },
            "data": [data_element]
        }

        width, height = size
        responses_element = []

        for band_name in band_names:
            responses_element.append({
                "identifier": band_name if len(band_names) > 1 else "default",
                "format": {
                    "type": "image/tiff"
                }
            })

        output_element = {
            "width": width,
            "height": height,
            "responses": responses_element
        }

        evalscript = []
        evalscript.extend([
            "//VERSION=3",
            "function setup() {",
            "    return {",
        ])
        if band_units:
            evalscript.extend([
                "        input: [{",
                "            bands: [" + ", ".join(map(repr, band_names)) + "],",
                "            units: [" + ", ".join(map(repr, band_units)) + "],",
                "        }],",
            ])
        else:
            evalscript.extend([
                "        input: [" + ", ".join(map(repr, band_names)) + "],",
            ])
        evalscript.extend([
            "        output: [",
        ])
        if len(band_names) > 1:
            evalscript.extend(
                ["            {id: " + repr(band_name) + ", bands: 1, sampleType: " + repr(sample_type) + "},"
                 for band_name, sample_type in zip(band_names, band_sample_types)])
        else:
            evalscript.extend(["            {bands: 1, sampleType: " + repr(band_sample_types[0]) + "}"])

        evalscript.extend([
            "        ]",
            "    };",
            "}"
        ])
        if len(band_names) > 1:
            evalscript.extend([
                "function evaluatePixel(sample) {",
                "    return {",
            ])
            evalscript.extend(["        " + band_name + ": [sample." + band_name + "]," for band_name in band_names])
            evalscript.extend([
                "    };",
                "}",
            ])
        else:
            evalscript.extend([
                "function evaluatePixel(sample) {",
                "    return [sample." + band_names[0] + "];",
                "}",
            ])

        # Convert into valid JSON
        return json.loads(json.dumps({
            "input": input_element,
            "output": output_element,
            "evalscript": "\n".join(evalscript)
        }))


class SentinelHubError(ValueError):
    def __init__(self, *args, response=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._response = response

    @property
    def response(self):
        return self._response

    @classmethod
    def maybe_raise_for_response(cls, response: requests.Response):
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            detail = None
            # noinspection PyBroadException
            try:
                data = response.json()
                if isinstance(data, dict):
                    detail = data.get('detail')
            except Exception:
                pass
            raise SentinelHubError(f'{e}: {detail}' if detail else f'{e}', response=response) from e


class SerializableOAuth2Session(requests_oauthlib.OAuth2Session):
    """
    Aids fixing an issue with using the SentinelHubStore when distributing across a dask cluster.
    The class requests_oauthlib.OAuth2Session does not implement the magic methods __getstate__ and __setstate__
    which are used during pickling.
    """
    _SERIALIZED_ATTRS = ['_client', 'compliance_hook', 'client_id', 'auto_refresh_url',
                         'auto_refresh_kwargs', 'scope', 'redirect_uri', 'cookies',
                         'trust_env', 'auth', 'headers', 'params', 'hooks', 'proxies',
                         'stream', 'cert', 'verify', 'max_redirects', 'adapters']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.auth = None

    def __getstate__(self):
        return {a: getattr(self, a) for a in self._SERIALIZED_ATTRS}

    def __setstate__(self, state):
        for a in self._SERIALIZED_ATTRS:
            setattr(self, a, state[a])
