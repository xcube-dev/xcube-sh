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
DEFAULT_SH_OAUTH2_URL = 'https://services.sentinel-hub.com/oauth'

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

SH_DATA_STORE_ID = 'sentinelhub'
SH_DATA_OPENER_ID = 'dataset:zarr:sentinelhub'
