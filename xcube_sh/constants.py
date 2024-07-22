# Copyright © 2022-2024 by the xcube development team and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import logging
import os

LOG = logging.getLogger("xcube.sh")

DEFAULT_CLIENT_ID = os.environ.get("SH_CLIENT_ID")
DEFAULT_CLIENT_SECRET = os.environ.get("SH_CLIENT_SECRET")

DEFAULT_SH_INSTANCE_URL = "https://services.sentinel-hub.com"

DEFAULT_SH_CDSE_INSTANCE_URL = "https://sh.dataspace.copernicus.eu"
DEFAULT_SH_CDSE_OAUTH2_URL = (
    "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect"
)

# SH Catalog only allows this number of features to requested.
SH_CATALOG_FEATURE_LIMIT = 100

DEFAULT_RETRY_BACKOFF_MAX = 40  # milliseconds
DEFAULT_RETRY_BACKOFF_BASE = 1.001
DEFAULT_NUM_RETRIES = 200

WGS84_CRS = "WGS84"
DEFAULT_CRS = WGS84_CRS
DEFAULT_BAND_UNITS = "DN"
# TODO: ask SIN, whether 10 minutes are OK
DEFAULT_TIME_TOLERANCE = "10m"  # 10 minutes

DEFAULT_RESAMPLING = "NEAREST"
RESAMPLINGS = "NEAREST", "BILINEAR", "BICUBIC"

DEFAULT_MOSAICKING_ORDER = "mostRecent"
MOSAICKING_ORDERS = "mostRecent", "leastRecent", "leastCC"

DEFAULT_TILE_SIZE = 1000

SH_MAX_IMAGE_SIZE = 2500

BAND_DATA_ARRAY_NAME = "band_data"

SH_DATA_STORE_ID = "sentinelhub"
SH_DATA_OPENER_ID = "dataset:zarr:sentinelhub"

SH_CDSE_DATA_STORE_ID = "sentinelhub-cdse"

AVAILABLE_CRS_EPSG_CODES = [
    4326,
    3857,
    2154,
    2180,
    2193,
    3003,
    3004,
    3031,
    3035,
    3346,
    3416,
    3765,
    3794,
    3844,
    3912,
    3995,
    4026,
    5514,
    28992,
    # UTM northern hemisphere:
    # The last two digits of EPSG codes above represent the number of
    # corresponding UTM zone in northern hemisphere,
    # e.g. use 32612 for UTM zone 12 North.
    *map(lambda zone: 32601 + zone, range(0, 60)),
    # UTM southern hemisphere:
    # e.g. use 32712 for UTM zone 12 South.
    *map(lambda zone: 32701 + zone, range(0, 60)),
]

# See https://docs.sentinel-hub.com/api/stage/api/process/crs/
CRS_ID_TO_URI = {
    "CRS84": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
    "WGS84": "http://www.opengis.net/def/crs/EPSG/0/4326",
    **{
        f"EPSG:{code}": f"http://www.opengis.net/def/crs/EPSG/0/{code}"
        for code in AVAILABLE_CRS_EPSG_CODES
    },
}

CRS_URI_TO_ID = {v: k for k, v in CRS_ID_TO_URI.items()}
CRS_URI_TO_ID.update(
    {
        "http://www.opengis.net/def/crs/OGC/1.3/CRS84": "CRS84",
        "http://www.opengis.net/def/crs/EPSG/0/4326": "WGS84",
    }
)
