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

import os

DEFAULT_INSTANCE_ID = os.environ.get('SH_INSTANCE_ID')
DEFAULT_CLIENT_ID = os.environ.get('SH_CLIENT_ID')
DEFAULT_CLIENT_SECRET = os.environ.get('SH_CLIENT_SECRET')

DEFAULT_SH_API_URL = 'https://services.sentinel-hub.com'
DEFAULT_SH_OAUTH2_URL = f'{DEFAULT_SH_API_URL}/oauth'

SH_CATALOG_FEATURE_LIMIT = 100  # SH Catalog only allows this number of features to requested.

DEFAULT_RETRY_BACKOFF_MAX = 40  # milliseconds
DEFAULT_RETRY_BACKOFF_BASE = 1.001
DEFAULT_NUM_RETRIES = 200

WGS84_CRS = 'http://www.opengis.net/def/crs/EPSG/0/4326'
DEFAULT_CRS = WGS84_CRS
DEFAULT_BAND_UNITS = 'DN'
DEFAULT_TIME_TOLERANCE = '10M'  # 10 minutes   TODO: ask SIN, whether 10 minutes are OK

DEFAULT_TILE_SIZE = 1000

SH_MAX_IMAGE_SIZE = 2500

BAND_DATA_ARRAY_NAME = 'band_data'

SH_ENDPOINTS = {
    "eu_central": "https://services.sentinel-hub.com/api/v1/catalog/collections",  # eu central
    "us_west": "https://services-uswest2.sentinel-hub.com/api/v1/catalog/collections",  # us west
    "creo": "https://creodias.sentinel-hub.com/api/v1/catalog/collections",  # CreaoDIAS
    "mundi": "https://shservices.mundiwebservices.com/api/v1/catalog/collections",  # Mundi
    "code_de": "https://code-de.sentinel-hub.com/api/v1/catalog/collections",
}

SH_DATA_STORE_ID = 'sentinelhub'
SH_DATA_OPENER_ID = 'dataset[cube]:zarr:sentinelhub'

# See https://docs.sentinel-hub.com/api/stage/api/process/crs/
AVAILABLE_CRS_IDS = [
    # WGS 84:
    'http://www.opengis.net/def/crs/OGC/1.3/CRS84',
    'http://www.opengis.net/def/crs/EPSG/0/4326',
    # WGS 84 / Pseudo-Mercator:
    'http://www.opengis.net/def/crs/EPSG/0/3857',
    'http://www.opengis.net/def/crs/EPSG/0/2154',
    'http://www.opengis.net/def/crs/EPSG/0/2180',
    'http://www.opengis.net/def/crs/EPSG/0/2193',
    'http://www.opengis.net/def/crs/EPSG/0/3003',
    'http://www.opengis.net/def/crs/EPSG/0/3004',
    'http://www.opengis.net/def/crs/EPSG/0/3031',
    'http://www.opengis.net/def/crs/EPSG/0/3035',
    'http://www.opengis.net/def/crs/EPSG/0/3346',
    'http://www.opengis.net/def/crs/EPSG/0/3416',
    'http://www.opengis.net/def/crs/EPSG/0/3765',
    'http://www.opengis.net/def/crs/EPSG/0/3794',
    'http://www.opengis.net/def/crs/EPSG/0/3844',
    'http://www.opengis.net/def/crs/EPSG/0/3912',
    'http://www.opengis.net/def/crs/EPSG/0/3995',
    'http://www.opengis.net/def/crs/EPSG/0/4026',
    'http://www.opengis.net/def/crs/EPSG/0/5514',
    'http://www.opengis.net/def/crs/EPSG/0/28992',
    'http://www.opengis.net/def/crs/SR-ORG/0/98739',
    # UTM northern hemisphere:
    # The last two digits of EPSG codes above represent the number of
    # corresponding UTM zone in northern hemisphere, e.g. use
    # http://www.opengis.net/def/crs/EPSG/0/32612 for UTM zone 12N.
    *map(lambda zone: 'http://www.opengis.net/def/crs/EPSG/0/326{:0>2d}'.format(
        zone + 1), range(0, 60)),
    # UTM southern hemisphere:
    *map(lambda zone: 'http://www.opengis.net/def/crs/EPSG/0/327{:0>2d}'.format(
        zone + 1), range(0, 60)),
]
