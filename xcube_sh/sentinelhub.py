# Copyright © 2022-2024 by the xcube development team and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

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
from .constants import DEFAULT_SH_INSTANCE_URL
from .constants import LOG
from .constants import SH_CATALOG_FEATURE_LIMIT
from .metadata import SentinelHubMetadata
from .version import version


class SentinelHub:
    """
    Represents the Sentinel Hub Cloud API.

    :param client_id: Sentinel Hub client ID
    :param client_secret: Sentinel Hub client secret
    :param instance_url:  Alternative Sentinel Hub instance URL.
    :param api_url: Deprecated, use instance_url instead.
    :param oauth2_url: Overrides default Sentinel Hub catalog API URL
        derived from *instance_url*.
    :param catalog_url: Overrides default Sentinel Hub catalog API URL
        derived from *instance_url*.
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

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        instance_id: Optional[str] = None,
        instance_url: Optional[str] = None,
        api_url: Optional[str] = None,
        oauth2_url: Optional[str] = None,
        process_url: Optional[str] = None,
        catalog_url: Optional[str] = None,
        collection_url: Optional[str] = None,
        configuration_url: Optional[str] = None,
        enable_warnings: bool = False,
        error_policy: str = "fail",
        error_handler: Optional[Callable[[Any], None]] = None,
        num_retries: int = DEFAULT_NUM_RETRIES,
        retry_backoff_max: int = DEFAULT_RETRY_BACKOFF_MAX,
        retry_backoff_base: float = DEFAULT_RETRY_BACKOFF_BASE,
        session: Union["SerializableOAuth2Session", Any] = None,
    ):
        if instance_id:
            warnings.warn(
                "instance_id has been deprecated," " it is no longer used",
                DeprecationWarning,
            )
        api_url = _get_url(api_url, None, "SH_API_URL")
        if api_url:
            warnings.warn(
                "Parameter api_url is deprecated, use instance_url instead.",
                category=DeprecationWarning,
            )
        instance_url = _get_url(
            instance_url, api_url or DEFAULT_SH_INSTANCE_URL, "SH_INSTANCE_URL"
        )
        self.instance_url = instance_url
        # The authorisation service many
        # SH instances is DEFAULT_SH_INSTANCE_URL!
        self.oauth2_url = _get_url(
            oauth2_url,
            f"{DEFAULT_SH_INSTANCE_URL}/oauth",  # !
            "SH_OAUTH2_URL",
        )
        self.process_url = _get_url(
            process_url,
            f"{instance_url}/api/v1/process",
            "SH_PROCESS_URL",
        )
        self.catalog_url = _get_url(
            catalog_url,
            f"{instance_url}/api/v1/catalog/1.0.0",
            "SH_CATALOG_URL",
        )
        self.configuration_url = _get_url(
            configuration_url,
            f"{instance_url}/configuration/v1",
            "SH_CONFIGURATION_URL",
        )
        self.collection_url = _get_url(
            collection_url,
            f"{instance_url}/api/v1/metadata/collection",
            "SH_COLLECTION_URL",
        )
        self.error_policy = error_policy or "fail"
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
            client = oauthlib.oauth2.BackendApplicationClient(client_id=self.client_id)
            self.session = SerializableOAuth2Session(client=client)
            self._fetch_token()

    def __del__(self):
        self.close()

    def close(self):
        self.session.close()

    @property
    def token_info(self) -> Dict[str, Any]:
        response = self.session.get(self.oauth2_url + "/tokeninfo")
        SentinelHubError.maybe_raise_for_response(response)
        return response.json()

    # noinspection PyMethodMayBeStatic
    @property
    def dataset_names(self) -> List[str]:
        return [item.get("id") for item in self.datasets]

    # noinspection PyMethodMayBeStatic
    @property
    def datasets(self) -> List[Dict[str, str]]:
        """
        See https://docs.sentinel-hub.com/api/latest/reference/#tag/configuration_dataset
        """
        response = self.session.get(f"{self.configuration_url}/datasets")
        SentinelHubError.maybe_raise_for_response(response)
        return response.json()

    def band_names(self, dataset_name: str, collection_id: str = None) -> List[str]:
        if dataset_name.upper() == "CUSTOM":
            url = f"{self.collection_url}/{collection_id}"
            response = self.session.get(url)
            SentinelHubError.maybe_raise_for_response(response)
            bands = response.json().get("bands", [])
            return [band.get("name") for band in bands]

        url = f"{self.process_url}/dataset/{dataset_name}/bands"
        response = self.session.get(url)
        SentinelHubError.maybe_raise_for_response(response)
        return response.json().get("data", {})

    def bands(
        self, dataset_name: str, collection_id: str = None
    ) -> List[Dict[str, Any]]:
        if dataset_name.upper() == "CUSTOM":
            url = f"{self.collection_url}/{collection_id}"
            response = self.session.get(url)
            SentinelHubError.maybe_raise_for_response(response)
            return response.json().get("bands", [])

        url = f"{self.process_url}/dataset/{dataset_name}/bands"
        response = self.session.get(url)
        SentinelHubError.maybe_raise_for_response(response)
        band_names = response.json().get("data", [])
        return [dict(name=band_name) for band_name in band_names]

    def collections(self) -> List[Dict[str, Any]]:
        """
        See https://docs.sentinel-hub.com/api/latest/reference/#operation/getCollections
        """
        response = self.session.get(f"{self.catalog_url}/collections")
        SentinelHubError.maybe_raise_for_response(response)
        return response.json().get("collections", [])

    def get_features(
        self,
        collection_name: str,
        bbox: Tuple[float, float, float, float] = None,
        crs: str = None,
        time_range: Tuple[str, str] = None,
        bad_request_ok: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Get geometric intersections of dataset given by *collection_name*
        with optional *bbox* and *time_range*. The result is returned
        as a list of features, whose properties include a "datetime" field.

        :param collection_name: dataset collection name
        :param bbox: bounding box
        :param crs: Name of a coordinate reference system of the coordinates
            given by *bbox*. Ignored if *bbox* is not given.
        :param time_range: time range
        :param bad_request_ok: return empty list rather than raise error
            on bad request
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
                exclude=["geometry", "bbox", "assets", "links"],
                include=["properties.datetime"],
            ),
        )
        if bbox:
            source_crs = pyproj.crs.CRS.from_string(crs or DEFAULT_CRS)
            if not source_crs.is_geographic:
                x1, y1, x2, y2 = bbox
                transformer = pyproj.Transformer.from_crs(
                    source_crs, "WGS84", always_xy=True
                )
                (x1, x2), (y1, y2) = transformer.transform((x1, x2), (y1, y2))
                bbox = x1, y1, x2, y2

            request.update(bbox=bbox)

        if time_range:

            def to_sh_format(dt: str) -> pd.Timestamp:
                dt = pd.to_datetime(dt, infer_datetime_format=True, utc=True)
                # SH wants the old-style UTC-'Z'
                # noinspection PyTypeChecker
                return dt.isoformat().replace("+00:00", "Z")

            t1, t2 = time_range
            if t1 or t2:
                request.update(
                    datetime=(
                        f'{to_sh_format(t1) if t1 else ".."}/'
                        f'{to_sh_format(t2) if t2 else ".."}'
                    )
                )

        search_url = f"{self.catalog_url}/search"

        all_features = []
        features_count = max_feature_count
        feature_offset = 0
        while features_count == max_feature_count:
            response = self.session.post(search_url, json=request)

            if bad_request_ok and response.status_code == 400:
                break

            SentinelHubError.maybe_raise_for_response(response)

            feature_collection = json.loads(response.content)
            if feature_collection.get("type") != "FeatureCollection" or not isinstance(
                feature_collection.get("features"), list
            ):
                print(80 * "=")
                print(feature_collection)
                print(80 * "=")
                raise SentinelHubError(
                    f"Got unexpected result from {response.url}", response=response
                )

            features = feature_collection["features"]
            if not features:
                break
            all_features.extend(features)
            features_count = len(features)
            feature_offset += features_count
            request.update(next=feature_offset)

        return all_features

    @classmethod
    def features_to_time_ranges(
        cls,
        features: List[Dict[str, Any]],
        max_timedelta: Union[str, pd.Timedelta] = "1H",
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        """
        Convert list of features from SH Catalog into list of time ranges
        whose time deltas are not greater than *max_timedelta*.

        :param features: Tile dictionaries as returned by SH WFS
        :param max_timedelta: Maximum time delta for each generated time range
        :return: List time range tuples.
        """
        max_timedelta = (
            pd.to_timedelta(max_timedelta)
            if isinstance(max_timedelta, str)
            else max_timedelta
        )

        timestamps = []
        for feature in features:
            if "properties" not in feature:
                continue
            properties = feature["properties"]
            if "datetime" not in properties:
                continue
            datetime = properties["datetime"]
            if not datetime:
                continue
            try:
                timestamps.append(pd.to_datetime(datetime, utc=True))
            except ValueError as e:
                warnings.warn(
                    f"failed parsing feature.properties.datetime: {e}", source=e
                )

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

    def get_data(self, request: Dict, mime_type=None) -> Optional[requests.Response]:
        if not mime_type:
            outputs = request["output"]["responses"]
            if len(outputs) > 1:
                mime_type = "application/tar"
            else:
                mime_type = outputs[0]["format"].get("type", "image/tiff")

        num_retries = self.num_retries
        retry_backoff_max = self.retry_backoff_max  # ms
        retry_backoff_base = self.retry_backoff_base

        process_url = self.process_url
        headers = self._get_request_headers(mime_type)

        response = None
        response_error = None
        last_retry = False
        start_time = time.time()

        for retry in range(num_retries):
            try:
                response = self.session.post(process_url, json=request, headers=headers)
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
                    error_message = (
                        f"Error {response.status_code}:" f" {response.reason}"
                    )
                    retry_min = int(response.headers.get("Retry-After", "100"))
                else:
                    error_message = f"Error: {response_error}"
                    retry_min = 100
                retry_backoff = random.random() * retry_backoff_max
                retry_total = retry_min + retry_backoff
                if self.enable_warnings:
                    retry_message = (
                        f"{error_message}. "
                        f"Attempt {retry + 1} of {num_retries} to retry after "
                        f'{"%.2f" % retry_min} + {"%.2f" % retry_backoff}'
                        f' = {"%.2f" % retry_total} ms...'
                    )
                    warnings.warn(retry_message)
                time.sleep(retry_total / 1000.0)
                retry_backoff_max *= retry_backoff_base

        end_time = time.time()

        # Here: response.ok == False

        if self.error_handler:
            self.error_handler(response)

        LOG.error(
            f"Failed to fetch data from SentinelHub"
            f" after {end_time - start_time} seconds"
            f" and {num_retries} retries",
            exc_info=response_error,
        )
        if response is not None:
            LOG.error(f"HTTP status code was {response.status_code}")

        if self.error_policy == "fail":
            if response_error:
                raise response_error
            elif response is not None:
                SentinelHubError.maybe_raise_for_response(response)
        elif self.error_policy == "warn" and self.enable_warnings:
            if response_error:
                warnings.warn(f"Failed to fetch data: {response_error}")
            elif response is not None:
                try:
                    SentinelHubError.maybe_raise_for_response(response)
                except SentinelHubError as e:
                    warnings.warn(f"Failed to fetch data: {e}")

        # Return failed response (response.ok == False)
        return response

    @classmethod
    def _get_request_headers(cls, mime_type: str):
        return {
            "Accept": mime_type,
            "SH-Tag": "xcube-sh",
            "User-Agent": f"xcube_sh/{version} "
            f"{platform.python_implementation()}/"
            f"{platform.python_version()} "
            f"{platform.system()}/{platform.version()}",
        }

    @classmethod
    def new_data_request(
        cls,
        dataset_name: str,
        band_names: Sequence[str],
        size: Tuple[int, int],
        crs: str = None,
        bbox: Tuple[float, float, float, float] = None,
        time_range: Tuple[Union[str, pd.Timestamp], Union[str, pd.Timestamp]] = None,
        upsampling: str = DEFAULT_RESAMPLING,
        downsampling: str = DEFAULT_RESAMPLING,
        mosaicking_order: str = DEFAULT_MOSAICKING_ORDER,
        collection_id: str = None,
        band_units: Union[str, Sequence[str]] = None,
        band_sample_types: Union[str, Sequence[str]] = None,
        processing_kwargs: dict = None,
    ) -> Dict:
        if bbox is None:
            bbox = [-180.0, -90.0, 180.0, 90.0]

        band_units = band_units or None
        if isinstance(band_units, str):
            band_units = [band_units] * len(band_names)

        band_sample_types = band_sample_types or "FLOAT32"
        if isinstance(band_sample_types, str):
            band_sample_types = [band_sample_types] * len(band_names)

        processing = {"upsampling": upsampling, "downsampling": downsampling}

        if processing_kwargs:
            processing |= processing_kwargs

        data_element = {
            "type": dataset_name if collection_id is None else collection_id,
            "processing": processing,
        }

        if any([time_range, mosaicking_order, collection_id]):
            data_element["dataFilter"] = dict()
            if time_range:
                time_range_from, time_range_to = time_range
                time_range_element = {}
                if time_range_from:
                    if not isinstance(time_range_from, str):
                        time_range_from = time_range_from.isoformat()
                    time_range_element["from"] = time_range_from
                if time_range_to:
                    if not isinstance(time_range_to, str):
                        time_range_to = time_range_to.isoformat()
                    time_range_element["to"] = time_range_to
                data_element["dataFilter"].update(timeRange=time_range_element)
            if mosaicking_order and time_range:
                data_element["dataFilter"].update(mosaickingOrder=mosaicking_order)

        input_element = {
            "bounds": {
                "bbox": bbox,
                "properties": {"crs": crs or CRS_ID_TO_URI[DEFAULT_CRS]},
            },
            "data": [data_element],
        }

        width, height = size
        responses_element = []

        responses_element.append(
            {"identifier": "default", "format": {"type": "image/tiff"}}
        )

        output_element = {
            "width": width,
            "height": height,
            "responses": responses_element,
        }

        evalscript = []
        evalscript.extend(
            [
                "//VERSION=3",
                "function setup() {",
                "    return {",
            ]
        )
        band_names_str = ", ".join(map(repr, band_names))
        if band_units:
            band_units_str = ", ".join(map(repr, band_units))
            evalscript.extend(
                [
                    "        input: [{",
                    "            bands: [" + band_names_str + "],",
                    "            units: [" + band_units_str + "]",
                    "        }],",
                ]
            )
        else:
            evalscript.extend(
                [
                    "        input: [{",
                    "            bands: [" + band_names_str + "]",
                    "        }],",
                ]
            )
        evalscript.extend(
            [
                "        output: [",
                "            {bands: "
                + str(len(band_names))
                + ", sampleType: "
                + repr(band_sample_types[0])
                + "}",
                "        ]",
                "    };",
                "}",
            ]
        )
        sample_strings = [f"sample.{band_name}" for band_name in band_names]
        evalscript.extend(
            [
                "function evaluatePixel(sample) {",
                "    return [" + ", ".join(sample_strings) + "];",
                "}",
            ]
        )

        # Convert into valid JSON
        return json.loads(
            json.dumps(
                {
                    "input": input_element,
                    "output": output_element,
                    "evalscript": "\n".join(evalscript),
                }
            )
        )

    def _fetch_token(self):
        if not self.client_id or not self.client_secret:
            raise ValueError(
                "Both client_id and client_secret must be provided.\n"
                "Consider setting environment variables "
                "SH_CLIENT_ID and SH_CLIENT_SECRET.\n"
                "For more information refer to "
                "https://docs.sentinel-hub.com/"
                "api/latest/#/API/authentication"
            )

        self.session.fetch_token(
            token_url=self.oauth2_url + "/token",
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

        LOG.info("fetched SentinelHub access token successfully")


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
                    # See https://github.com/dcs4cop/xcube-sh/issues/100
                    detail = data.get("detail") or data.get("description")
            except Exception:
                pass
            raise SentinelHubError(
                f"{e}: {detail}" if detail else f"{e}", response=response
            ) from e


class SerializableOAuth2Session(requests_oauthlib.OAuth2Session):
    """
    Aids fixing an issue with using the SentinelHubStore when distributing
    across a dask cluster.
    The class requests_oauthlib.OAuth2Session does not implement the
    magic methods __getstate__ and __setstate__
    which are used during pickling.
    """

    _SERIALIZED_ATTRS = [
        "_client",
        "compliance_hook",
        "client_id",
        "auto_refresh_url",
        "auto_refresh_kwargs",
        "scope",
        "redirect_uri",
        "cookies",
        "trust_env",
        "auth",
        "headers",
        "params",
        "hooks",
        "proxies",
        "stream",
        "cert",
        "verify",
        "max_redirects",
        "adapters",
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.auth = None

    def __getstate__(self):
        return {a: getattr(self, a) for a in self._SERIALIZED_ATTRS}

    def __setstate__(self, state):
        for a in self._SERIALIZED_ATTRS:
            setattr(self, a, state[a])


def _get_url(url: Optional[str], default_url: Optional[str], env_var: str) -> str:
    return url if url else os.environ.get(env_var, default_url)
