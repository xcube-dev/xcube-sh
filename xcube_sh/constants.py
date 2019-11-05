import os

DEFAULT_INSTANCE_ID = os.environ.get('SH_INSTANCE_ID')
DEFAULT_CLIENT_ID = os.environ.get('SH_CLIENT_ID')
DEFAULT_CLIENT_SECRET = os.environ.get('SH_CLIENT_SECRET')

DEFAULT_SH_API_URL = 'https://services.sentinel-hub.com/api/v1'
DEFAULT_SH_OAUTH2_URL = 'https://services.sentinel-hub.com/oauth'

DEFAULT_CRS = 'http://www.opengis.net/def/crs/EPSG/0/4326'
DEFAULT_BAND_UNITS = 'DN'
DEFAULT_TIME_TOLERANCE = '10M'   # 10 minutes   TODO: ask SIN, whether 10 minutes are OK

BAND_DATA_ARRAY_NAME = 'band_data'

SH_MAX_IMAGE_SIZE = 2500

DEFAULT_NUM_CHUNK_ELEMENTS = 1000 * 1000
