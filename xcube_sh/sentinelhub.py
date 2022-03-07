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

import json
import os
import platform
import random
import time
import warnings
from typing import List, Any, Dict, Tuple, Union, Sequence, Callable, Optional

import oauthlib.oauth2
import pandas as pd
import pyproj
import requests
import requests_oauthlib

from .constants import CRS_ID_TO_URI
from .constants import DEFAULT_CLIENT_ID
from .constants import DEFAULT_CLIENT_SECRET
from .constants import DEFAULT_CRS
from .constants import DEFAULT_MOSAICKING_ORDER
from .constants import DEFAULT_NUM_RETRIES
from .constants import DEFAULT_RESAMPLING
from .constants import DEFAULT_RETRY_BACKOFF_BASE
from .constants import DEFAULT_RETRY_BACKOFF_MAX
from .constants import DEFAULT_SH_API_URL
from .constants import DEFAULT_SH_METADATA_API_URL
from .constants import DEFAULT_SH_OAUTH2_URL
from .constants import SH_CATALOG_FEATURE_LIMIT
from .metadata import SentinelHubMetadata
from .version import version


class SentinelHub:
    """
    Represents the SENTINEL Hub Cloud API.

    :param client_id: SENTINEL Hub client ID
    :param client_secret: SENTINEL Hub client secret
    :param instance_id:  SENTINEL Hub instance
        ID (deprecated, no longer used)
    :param api_url: Alternative SENTINEL Hub API URL.
    :param oauth2_url: Alternative SENTINEL Hub OAuth2 API URL.
    :param process_url: Overrides default SH process API URL
        derived from *api_url*.
    :param catalog_url: Overrides default SH catalog API URL
        derived from *api_url*.
    :param error_policy: "raise" or "warn".
        If "raise" an exception is raised on failed API requests.
    :param error_handler: An optional function called with the
        response from a failed API request.
    :param enable_warnings: Allow emitting warnings on failed API requests.
    :param num_retries: Number of retries for failed API
        requests, e.g. ```50`` times.
    :param retry_backoff_max: Request retry backoff
        time in milliseconds, e.g. ``100`` milliseconds
    :param retry_backoff_base:  Request retry backoff base.
        Must be greater than one, e.g. ``1.5``
    :param session: Optional request session object (mostly for testing).
    """

    METADATA = SentinelHubMetadata()

    def __init__(self,
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 instance_id: Optional[str] = None,
                 api_url: Optional[str] = None,
                 oauth2_url: Optional[str] = None,
                 process_url: Optional[str] = None,
                 catalog_url: Optional[str] = None,
                 enable_warnings: bool = False,
                 error_policy: str = 'fail',
                 error_handler: Optional[Callable[[Any], None]] = None,
                 num_retries: int = DEFAULT_NUM_RETRIES,
                 retry_backoff_max: int = DEFAULT_RETRY_BACKOFF_MAX,
                 retry_backoff_base: float = DEFAULT_RETRY_BACKOFF_BASE,
                 session: Union["SerializableOAuth2Session", Any] = None):
        if instance_id:
            warnings.warn('instance_id has been deprecated,'
                          ' it is no longer used')
        self.api_url = api_url or os.environ.get('SH_API_URL',
                                                 DEFAULT_SH_API_URL)
        self.oauth2_url = oauth2_url or os.environ.get('SH_OAUTH2_URL',
                                                       DEFAULT_SH_OAUTH2_URL)
        self.token_url = self.oauth2_url + '/token'
        self.process_url = process_url
        self.catalog_url = catalog_url
        self.error_policy = error_policy or 'fail'
        self.error_handler = error_handler
        self.enable_warnings = enable_warnings
        self.num_retries = num_retries
        self.retry_backoff_max = retry_backoff_max
        self.retry_backoff_base = retry_backoff_base
        self.session: Optional[SerializableOAuth2Session] = session
        # Client credentials
        self.client_id = client_id or DEFAULT_CLIENT_ID
        self.client_secret = client_secret or DEFAULT_CLIENT_SECRET
        if session is None:
            # Create a OAuth2 session
            client = oauthlib.oauth2.BackendApplicationClient(
                client_id=self.client_id
            )
            self.session = SerializableOAuth2Session(client=client)
            self._fetch_token()

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
        response = self.session.get(
            self.api_url + '/configuration/v1/datasets'
        )
        SentinelHubError.maybe_raise_for_response(response)
        return response.json()

    def band_names(self, dataset_name: str, collection_id: str = None) \
            -> List[str]:
        if dataset_name.upper() == 'CUSTOM':
            url = DEFAULT_SH_METADATA_API_URL % collection_id
            response = self.session.get(url)
            SentinelHubError.maybe_raise_for_response(response)
            bands = response.json().get('bands', [])
            return [band.get('name') for band in bands]

        url = f'{self.api_url}/api/v1/process/dataset/{dataset_name}/bands'
        response = self.session.get(url)
        SentinelHubError.maybe_raise_for_response(response)
        return response.json().get('data', {})

    def bands(self, dataset_name: str, collection_id: str = None) \
            -> List[Dict[str, Any]]:
        if dataset_name.upper() == 'CUSTOM':
            url = DEFAULT_SH_METADATA_API_URL % collection_id
            response = self.session.get(url)
            SentinelHubError.maybe_raise_for_response(response)
            return response.json().get('bands', [])

        url = f'{self.api_url}/api/v1/process/dataset/{dataset_name}/bands'
        response = self.session.get(url)
        SentinelHubError.maybe_raise_for_response(response)
        band_names = response.json().get('data', [])
        return [dict(name=band_name) for band_name in band_names]

    def collections(self) -> List[Dict[str, Any]]:
        """
        See https://docs.sentinel-hub.com/api/latest/reference/#operation/getCollections
        """
        response = self.session.get(
            f'{self.api_url}/api/v1/catalog/collections'
        )
        SentinelHubError.maybe_raise_for_response(response)
        return response.json().get('collections', [])

    def get_features(self,
                     collection_name: str,
                     bbox: Tuple[float, float, float, float] = None,
                     crs: str = None,
                     time_range: Tuple[str, str] = None) \
            -> List[Dict[str, Any]]:
        """
        Get geometric intersections of dataset given by *collection_name*
        with optional *bbox* and *time_range*. The result is returned
        as a list of features, whose properties include a "datetime" field.

        :param collection_name: dataset collection name
        :param bbox: bounding box
        :param crs: Name of a coordinate reference system of the coordinates
            given by *bbox*. Ignored if *bbox* is not given.
        :param time_range: time range
        :return: list of features that include a "datetime" field for
            all intersections.
        """
        max_feature_count = SH_CATALOG_FEATURE_LIMIT

        request = dict(
            collections=[collection_name],
            limit=max_feature_count,
            # Exclude most of the response data,
            # as this is not required (yet)
            fields=dict(
                exclude=['geometry', 'bbox', 'assets', 'links'],
                include=['properties.datetime']
            )
        )
        if bbox:
            source_crs = pyproj.crs.CRS.from_string(crs or DEFAULT_CRS)
            if not source_crs.is_geographic:
                x1, y1, x2, y2 = bbox
                transformer = pyproj.Transformer.from_crs(source_crs,
                                                          'WGS84',
                                                          always_xy=True)
                (x1, x2), (y1, y2) = transformer.transform((x1, x2), (y1, y2))
                bbox = x1, y1, x2, y2

            request.update(bbox=bbox)

        if time_range:
            def to_sh_format(dt: str) -> pd.Timestamp:
                dt = pd.to_datetime(dt,
                                    infer_datetime_format=True,
                                    utc=True)
                # SH wants the old-style UTC-'Z'
                # noinspection PyTypeChecker
                return dt.isoformat().replace('+00:00', 'Z')

            t1, t2 = time_range
            if t1 or t2:
                request.update(
                    datetime=f'{to_sh_format(t1) if t1 else ".."}/'
                             f'{to_sh_format(t2) if t2 else ".."}'
                )

        catalog_url = self.catalog_url \
                      or f'{self.api_url}/api/v1/catalog/search'
        headers = self._get_request_headers('application/json')

        all_features = []
        features_count = max_feature_count
        feature_offset = 0
        while features_count == max_feature_count:
            response = self.session.post(catalog_url,
                                         json=request,
                                         headers=headers)

            SentinelHubError.maybe_raise_for_response(response)

            feature_collection = json.loads(response.content)
            if feature_collection.get('type') != 'FeatureCollection' \
                    or not isinstance(feature_collection.get('features'),
                                      list):
                raise SentinelHubError(f'Got unexpected'
                                       f' result from {response.url}',
                                       response=response)

            features = feature_collection['features']
            all_features.extend(features)
            features_count = len(features)
            feature_offset += features_count
            request.update(next=feature_offset)

        return all_features

    @classmethod
    def features_to_time_ranges(
            cls,
            features: List[Dict[str, Any]],
            max_timedelta: Union[str, pd.Timedelta] = '1H'
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        """
        Convert list of features from SH Catalog into list of time ranges
        whose time deltas are not greater than *max_timedelta*.

        :param features: Tile dictionaries as returned by SH WFS
        :param max_timedelta: Maximum time delta for each generated time range
        :return: List time range tuples.
        """
        max_timedelta = pd.to_timedelta(max_timedelta) \
            if isinstance(max_timedelta, str) else max_timedelta

        timestamps = []
        for feature in features:
            if 'properties' not in feature:
                continue
            properties = feature['properties']
            if 'datetime' not in properties:
                continue
            datetime = properties['datetime']
            if not datetime:
                continue
            try:
                timestamps.append(pd.to_datetime(datetime, utc=True))
            except ValueError as e:
                warnings.warn(f'failed parsing'
                              f' feature.properties.datetime: {e}',
                              source=e)

        timestamps = sorted(set(timestamps))
        num_timestamps = len(timestamps)

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

    def get_data(self, request: Dict, mime_type=None) \
            -> Optional[requests.Response]:
        if not mime_type:
            outputs = request['output']['responses']
            if len(outputs) > 1:
                mime_type = 'application/tar'
            else:
                mime_type = outputs[0]['format'].get('type', 'image/tiff')

        num_retries = self.num_retries
        retry_backoff_max = self.retry_backoff_max  # ms
        retry_backoff_base = self.retry_backoff_base

        process_url = self.process_url or f'{self.api_url}/api/v1/process'
        headers = self._get_request_headers(mime_type)

        response = None
        response_error = None
        last_retry = False
        for retry in range(num_retries):
            try:
                response = self.session.post(process_url,
                                             json=request,
                                             headers=headers)
                response_error = None
            except oauthlib.oauth2.TokenExpiredError as e:
                if not last_retry and retry == num_retries - 1:
                    # Force a last retry
                    last_retry = True
                    retry -= 1
                self._fetch_token()
                response_error = e
                response = None
            except requests.exceptions.RequestException as e:
                # What may be seen here is:
                # requests.exceptions.ChunkedEncodingError:
                # ("Connection broken:
                #  InvalidChunkLength(got length b'', 0 bytes read)",
                #  InvalidChunkLength(got length b'', 0 bytes read))
                response_error = e
                response = None
            if response is not None and response.status_code == 401:
                if not last_retry and retry == num_retries - 1:
                    # Force a last retry
                    last_retry = True
                    retry -= 1
                self._fetch_token()
            if response is not None and response.ok:
                # TODO (forman): verify response headers:
                #   response_num_components, response_width, ...
                # response_components = int(headers.get('SH-Components','-1'))
                # response_width = int(headers.get('SH-Width', '-1'))
                # response_height = int(headers.get('SH-Height', '-1'))
                # response_sample_type = headers.get('SH-SampleType')
                return response
            else:
                # Retry after 'Retry-After' with exponential backoff
                if response is not None:
                    error_message = f'Error {response.status_code}:' \
                                    f' {response.reason}'
                    retry_min = int(response.headers.get(
                        'Retry-After', '100'
                    ))
                else:
                    error_message = f'Error: {response_error}'
                    retry_min = 100
                retry_backoff = random.random() * retry_backoff_max
                retry_total = retry_min + retry_backoff
                if self.enable_warnings:
                    retry_message = \
                        f'{error_message}. ' \
                        f'Attempt {retry + 1} of {num_retries} to retry after ' \
                        f'{"%.2f" % retry_min} + {"%.2f" % retry_backoff}' \
                        f' = {"%.2f" % retry_total} ms...'
                    warnings.warn(retry_message)
                time.sleep(retry_total / 1000.0)
                retry_backoff_max *= retry_backoff_base

        if self.error_handler:
            self.error_handler(response)

        if self.error_policy == 'fail':
            if response_error:
                raise response_error
            elif response is not None:
                SentinelHubError.maybe_raise_for_response(response)
        elif self.error_policy == 'warn' and self.enable_warnings:
            if response_error:
                warnings.warn(f'Failed to fetch data: {response_error}')
            elif response is not None:
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
            'SH-Tag': 'xcube-sh',
            'User-Agent': f'xcube_sh/{version} '
                          f'{platform.python_implementation()}/'
                          f'{platform.python_version()} '
                          f'{platform.system()}/{platform.version()}'
        }

    @classmethod
    def new_data_request(
            cls,
            dataset_name: str,
            band_names: Sequence[str],
            size: Tuple[int, int],
            crs: str = None,
            bbox: Tuple[float, float, float, float] = None,
            time_range: Tuple[Union[str, pd.Timestamp],
                              Union[str, pd.Timestamp]] = None,
            upsampling: str = DEFAULT_RESAMPLING,
            downsampling: str = DEFAULT_RESAMPLING,
            mosaicking_order: str = DEFAULT_MOSAICKING_ORDER,
            collection_id: str = None,
            band_units: Union[str, Sequence[str]] = None,
            band_sample_types: Union[str, Sequence[str]] = None
    ) -> Dict:

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
                data_element["dataFilter"].update(
                    timeRange=time_range_element
                )
            if mosaicking_order and time_range:
                data_element["dataFilter"].update(
                    mosaickingOrder=mosaicking_order
                )
            if collection_id:
                data_element["dataFilter"].update(
                    collectionId=collection_id
                )

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
            band_names_str = ", ".join(map(repr, band_names))
            band_units_str = ", ".join(map(repr, band_units))
            evalscript.extend([
                "        input: [{",
                "            bands: [" + band_names_str + "],",
                "            units: [" + band_units_str + "],",
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
                ["            {id: "
                 + repr(band_name)
                 + ", bands: 1, sampleType: "
                 + repr(sample_type) + "},"
                 for band_name, sample_type in zip(band_names,
                                                   band_sample_types)])
        else:
            evalscript.extend(["            {bands: 1, sampleType: "
                               + repr(band_sample_types[0]) + "}"])

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
            evalscript.extend(["        "
                               + band_name
                               + ": [sample."
                               + band_name + "],"
                               for band_name in band_names])
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

    def _fetch_token(self):
        if not self.client_id or not self.client_secret:
            raise ValueError(
                'Both client_id and client_secret must be provided.\n'
                'Consider setting environment variables '
                'SH_CLIENT_ID and SH_CLIENT_SECRET.\n'
                'For more information refer to '
                'https://docs.sentinel-hub.com/'
                'api/latest/#/API/authentication'
            )

        self.session.fetch_token(
            token_url=self.oauth2_url + '/token',
            client_id=self.client_id,
            client_secret=self.client_secret
        )


class SentinelHubError(ValueError):
    def __init__(self, *args, response=None, **kwargs):
        # noinspection PyArgumentList
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
            raise SentinelHubError(f'{e}: {detail}' if detail else f'{e}',
                                   response=response) from e


class SerializableOAuth2Session(requests_oauthlib.OAuth2Session):
    """
    Aids fixing an issue with using the SentinelHubStore when distributing
    across a dask cluster.
    The class requests_oauthlib.OAuth2Session does not implement the
    magic methods __getstate__ and __setstate__
    which are used during pickling.
    """
    _SERIALIZED_ATTRS = [
        '_client', 'compliance_hook', 'client_id', 'auto_refresh_url',
        'auto_refresh_kwargs', 'scope', 'redirect_uri', 'cookies',
        'trust_env', 'auth', 'headers', 'params', 'hooks', 'proxies',
        'stream', 'cert', 'verify', 'max_redirects', 'adapters'
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.auth = None

    def __getstate__(self):
        return {a: getattr(self, a) for a in self._SERIALIZED_ATTRS}

    def __setstate__(self, state):
        for a in self._SERIALIZED_ATTRS:
            setattr(self, a, state[a])
