# The MIT License (MIT)
# Copyright (c) 2021 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
Static SH metadata.
"""

from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse


class SentinelHubMetadata:

    def __init__(self):
        self._metadata = _SH_METADATA
        self._extra_collections = _EXTRA_COLLECTIONS

    @property
    def datasets(self) -> Dict:
        return dict(self._metadata['datasets'])

    @property
    def collection_datasets(self) -> Dict:
        return {v['collection_name']: dict(**v, dataset_name=k)
                for k, v in self.datasets.items()
                if 'collection_name' in v}

    def extra_collections(self, api_url: str) -> List[Dict[str, Any]]:
        endpoint = urlparse(api_url).hostname
        return list(self._extra_collections.get(endpoint, []))

    @property
    def dataset_names(self) -> List[str]:
        return [ds_id for ds_id in self._metadata['datasets']]

    def dataset(self, dataset_name: str) -> Optional[Dict]:
        dataset = self._dataset_direct(dataset_name)
        return dict(dataset) if dataset is not None else None

    def dataset_title(self, dataset_name: str, default=None) -> Optional[str]:
        dataset = self._dataset_direct(dataset_name)
        return dataset.get('title', default) if dataset else default

    def dataset_processing_level(self, dataset_name: str, default=None) -> Optional[str]:
        dataset = self._dataset_direct(dataset_name)
        return dataset.get('processing_level', default) if dataset else default

    def dataset_request_period(self, dataset_name: str, default=None) -> Optional[str]:
        dataset = self._dataset_direct(dataset_name)
        return dataset.get('request_period', default) if dataset else default

    def dataset_collection_name(self, dataset_name: str, default=None) -> Optional[str]:
        dataset = self._dataset_direct(dataset_name)
        return dataset.get('collection_name', default) if dataset else default

    def dataset_bands(self, dataset_name: str, default=None) -> Optional[Dict]:
        bands = self._dataset_bands_direct(dataset_name)
        return dict(bands) if bands else default

    def dataset_band_names(self, dataset_name: str, default=None) -> Optional[List[str]]:
        bands = self._dataset_bands_direct(dataset_name)
        return [var_name for var_name in bands] if bands else default

    def dataset_band(self, dataset_name: str, band_name: str, default=None) -> Optional[Dict]:
        band = self._dataset_band_direct(dataset_name, band_name)
        return dict(band) if band else default

    def dataset_band_sample_type(self, dataset_name: str, band_name: str, default='FLOAT32') -> str:
        band = self._dataset_band_direct(dataset_name, band_name)
        return band.get('sample_type', default) if band else default

    def dataset_band_fill_value(self, dataset_name: str, band_name: str, default=None) -> Optional[Union[int, float]]:
        band = self._dataset_band_direct(dataset_name, band_name)
        return band.get('fill_value', default) if band else default

    def _dataset_direct(self, dataset_name: str) -> Optional[Dict]:
        return self._metadata['datasets'].get(dataset_name)

    def _dataset_bands_direct(self, dataset_name: str) -> Optional[Dict]:
        dataset = self._dataset_direct(dataset_name)
        return dataset.get('bands') if dataset else None

    def _dataset_band_direct(self, dataset_name: str, band_name: str) -> Optional[Dict]:
        bands = self._dataset_bands_direct(dataset_name)
        return bands.get(band_name) if bands else None


_DEM_COLLECTION_NAME = "dem"

_DEM_COLLECTION = {
    "id": _DEM_COLLECTION_NAME,
    "title": "Digital Elevation Model",
    "description": "Digital elevation model data by Mapzen",
    "extent": {
        "spatial": {
            "bbox": (-180.0, -90.0, 180.0, 90.0),
        }
    }
}

# Mapping from SH endpoints to collection metadata using STAC metadata subset.
# Only datasets not accessible through Catalog collection are provided here.
#
_EXTRA_COLLECTIONS: Dict[str, List[Dict[str, Any]]] = {
    # EU Central
    "services.sentinel-hub.com": [_DEM_COLLECTION],
    # US West
    "services-uswest2.sentinel-hub.com": [_DEM_COLLECTION],
    # CreaoDIAS
    "creodias.sentinel-hub.com": [],
    # Mundi
    "shservices.mundiwebservices.com": [],
    # CODE-DE
    "code-de.sentinel-hub.com": [],
}

S1GRD_BAND_NAMES = ['VV', 'VH', 'HV', 'HH']

S1GRD_BAND_METADATA = {S1GRD_BAND_NAMES[i]: dict(sample_type='FLOAT32',
                                                 units='Linear power in the chosen backscattering coefficient',
                                                 # fill_value=0
                                                 )
                       for i in range(len(S1GRD_BAND_NAMES))}

S2_BAND_NAMES = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B09', 'B10', 'B11', 'B12']
S2A_WAVELENGTHS = [442.7, 492.4, 559.8, 664.6, 704.1, 740.5, 782.8, 832.8, 864.7, 945.1, 1373.5, 1613.7, 2202.4]
S2A_BANDWIDTHS = [21, 66, 36, 31, 15, 15, 20, 106, 21, 20, 31, 91, 175]
S2B_WAVELENGTHS = [442.2, 492.1, 559, 664.9, 703.8, 739.1, 779.7, 832.9, 864, 943.2, 1376.9, 1610.4, 2185.7]
S2B_BANDWIDTHS = [21, 66, 36, 31, 16, 15, 20, 106, 22, 21, 30, 94, 185]
S2_RESOLUTIONS = [60, 10, 10, 10, 20, 20, 20, 10, 20, 60, 60, 20, 20]

S2_BAND_METADATA = {S2_BAND_NAMES[i]: dict(sample_type='FLOAT32',
                                           units='reflectance',
                                           wavelength=round(0.5 * (S2A_WAVELENGTHS[i] + S2B_WAVELENGTHS[i]), 2),
                                           wavelength_a=S2A_WAVELENGTHS[i],
                                           wavelength_b=S2B_WAVELENGTHS[i],
                                           bandwidth=round(0.5 * (S2A_BANDWIDTHS[i] + S2B_BANDWIDTHS[i]), 2),
                                           bandwidth_a=S2A_BANDWIDTHS[i],
                                           bandwidth_b=S2B_BANDWIDTHS[i],
                                           resolution=S2_RESOLUTIONS[i],
                                           fill_value=0.0)
                    for i in range(len(S2_BAND_NAMES))}

S2_ANGLE_METADATA = {
    'viewZenithMean': dict(sample_type='FLOAT32'),
    'viewAzimuthMean': dict(sample_type='FLOAT32'),
    'sunZenithAngles': dict(sample_type='FLOAT32'),
    'sunAzimuthAngles': dict(sample_type='FLOAT32'),
}

S2L1C_BAND_METADATA = S2_BAND_METADATA.copy()
S2L1C_BAND_METADATA.update(S2_ANGLE_METADATA)

S2L2A_BAND_METADATA = S2_BAND_METADATA.copy()
S2L2A_BAND_METADATA.update(S2_ANGLE_METADATA)
S2L2A_SLC_MEANINGS = ['no_data',
                      'saturated_or_defective',
                      'dark_area_pixels',
                      'cloud_shadows',
                      'vegetation',
                      'bare_soils',
                      'water',
                      'clouds_low_probability_or_unclassified',
                      'clouds_medium_probability',
                      'clouds_high_probability',
                      'cirrus',
                      'snow_or_ice']

S2L2A_BAND_METADATA.update({
    "AOT": dict(sample_type='FLOAT32'),
    "SCL": dict(sample_type='UINT8',
                flag_values=','.join(f'{i}' for i in range(len(S2L2A_SLC_MEANINGS))),
                flag_meanings=' '.join(S2L2A_SLC_MEANINGS)),
    "SNW": dict(sample_type='UINT8'),
    "CLD": dict(sample_type='UINT8'),
})

S3OLCI_BAND_NAMES = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B10', 'B11',
                     'B12', 'B13', 'B14', 'B15', 'B16', 'B17', 'B18', 'B19', 'B20', 'B21']
S3OLCI_WAVELENGTHS = [400, 412.5, 442.5, 490, 510, 560, 620, 665, 673.75, 681.25, 708.75, 753.75, 761.25, 764.375,
                      767.5, 778.75, 865, 885, 900, 940, 1020]
# bandwidths info https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-3-olci/resolutions/radiometric
S3OLCI_BANDWIDTHS = [15, 10, 10, 10, 10, 10, 10, 10, 10, 7.5, 7.5, 10, 7.5, 2.5, 3.75, 2.5, 15, 20, 10, 10, 20, 40]

S3OLCI_BAND_METADATA = {S3OLCI_BAND_NAMES[i]: dict(sample_type='FLOAT32',
                                                   units='reflectance',
                                                   wavelength=S3OLCI_WAVELENGTHS[i],
                                                   bandwidth=S3OLCI_BANDWIDTHS[i],
                                                   resolution=300,
                                                   fill_value=65535.0
                                                   )
                        for i in range(len(S3OLCI_BAND_NAMES))}

S3OLCI_QUALITY_FLAG_NAMES = ['land', 'coastline', 'fresh_inland_water', 'tidal_region', 'bright', 'straylight_risk',
                             'invalid', 'cosmetic', 'duplicated', 'sun_glint_risk', 'dubious', 'saturated_Oa01',
                             'saturated_Oa02', 'saturated_Oa03', 'saturated_Oa04', 'saturated_Oa05', 'saturated_Oa06',
                             'saturated_Oa07', 'saturated_Oa08', 'saturated_Oa09', 'saturated_Oa10', 'saturated_Oa11',
                             'saturated_Oa12', 'saturated_Oa13', 'saturated_Oa14', 'saturated_Oa15', 'saturated_Oa16',
                             'saturated_Oa17', 'saturated_Oa18', 'saturated_Oa19', 'saturated_Oa20', 'saturated_Oa21'
                             ]

S3OLCI_QUALITY_FLAG_COUNT = len(S3OLCI_QUALITY_FLAG_NAMES)

S3OLCI_BAND_METADATA.update({
    "quality_flags": dict(sample_type='UINT32',
                          flag_values=','.join(
                              f'{(1 << (S3OLCI_QUALITY_FLAG_COUNT - 1 - i))}' for i in
                              range(S3OLCI_QUALITY_FLAG_COUNT)),
                          flag_meanings=' '.join(S3OLCI_QUALITY_FLAG_NAMES))
})

S3SLSTR_BAND_NAMES = ['S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8', 'S9', 'F1', 'F2']
S3SLSTR_WAVELENGTHS = [554.27, 659.47, 868, 1374.80, 1613.40, 2255.70, 3742, 10854, 12022.50, 3742, 10854]
S3SLSTR_RESOLUTIONS = [500, 500, 500, 500, 500, 500, 1000, 1000, 1000, 1000, 1000]
S3SLSTR_UNITS = ['reflectance', 'reflectance', 'reflectance', 'reflectance', 'reflectance', 'reflectance',
                 'kelvin', 'kelvin', 'kelvin', 'kelvin', 'kelvin']
# bandwidths info https://sentinel.esa.int/web/sentinel/user-guides/sentinel-3-slstr/resolutions/radiometric
S3SLSTR_BANDWIDTHS = [19.26, 19.25, 20.60, 20.80, 60.68, 50.15, 398.00, 776.00, 905.00, 398.00, 776.00]

S3SLSTR_BAND_METADATA = {S3SLSTR_BAND_NAMES[i]: dict(sample_type='UINT16',
                                                     units=S3SLSTR_UNITS[i],
                                                     wavelength=S3SLSTR_WAVELENGTHS[i],
                                                     bandwidth=S3SLSTR_BANDWIDTHS[i],
                                                     resolution=S3SLSTR_RESOLUTIONS[i],
                                                     fill_value=-32768
                                                     )
                         for i in range(len(S3SLSTR_BAND_NAMES))}
# TODO: Write down available flags - Have fun, there are 60 of them each one of them containing a number of different
#  codings.
# S3SLSTR_QUALITY_FLAG_NAMES =[]
# S3SLSTR_QUALITY_FLAG_VALUES=[]


S5PL2_BAND_NAMES = ['CO', 'HCHO', 'NO2', 'O3', 'SO2', 'CH4', 'AER_AI_340_380', 'AER_AI_354_388', 'CLOUD_BASE_PRESSURE',
                    'CLOUD_TOP_PRESSURE', 'CLOUD_BASE_HEIGHT', 'CLOUD_TOP_HEIGHT', 'CLOUD_OPTICAL_THICKNESS',
                    'CLOUD_FRACTION']
S5PL2_UNITS = ['mol/m^2', 'mol/m^2', 'mol/m^2', 'mol/m^2', 'mol/m^2', 'parts per billion', 'Unitless', 'Unitless',
               'Pascals',
               'Pascals', 'Meters', 'Meters', 'Unitless', 'Unitless']

S5PL2_BAND_METADATA = {S5PL2_BAND_NAMES[i]: dict(sample_type='FLOAT32',
                                                 units=S5PL2_UNITS[i],
                                                 # fill_value=0.0
                                                 )
                       for i in range(len(S5PL2_BAND_NAMES))}

L8L1C_BAND_NAMES = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B10', 'B11', 'BQA']
L8L1C_WAVELENGTHS = [443, 482, 561.5, 654.5, 865, 1608.5, 2200.5, 589.5, 1373.5, 10895, 12005, float('nan')]
L8L1C_RESOLUTIONS = [30, 30, 30, 30, 30, 30, 30, 15, 30, 100, 100, 30]
L8L1C_UNITS = ['reflectance', 'reflectance', 'reflectance', 'reflectance', 'reflectance', 'reflectance', 'reflectance',
               'reflectance', 'reflectance', 'kelvin', 'kelvin', 'Unitless']

L8L1C_BAND_METADATA = {L8L1C_BAND_NAMES[i]: dict(sample_type='UINT16',
                                                 units=L8L1C_UNITS[i],
                                                 wavelength=L8L1C_WAVELENGTHS[i],
                                                 resolution=L8L1C_RESOLUTIONS[i],
                                                 # fill_value=0.0
                                                 )
                       for i in range(len(L8L1C_BAND_NAMES))}

LOTL1_BAND_NAMES = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B10', 'B11', 'BQA', 'QA_RADSAT',
                    'VAA', 'VZA', 'SAA', 'SZA']
LOTL1_WAVELENGTHS = [443, 482, 561.5, 654.5, 865, 1608.5, 2200.5, 589.5, 1373.5, 10895, 12005, float('nan'),
                     float('nan'), float('nan'), float('nan'), float('nan'), float('nan')]
LOTL1_RESOLUTIONS = [30, 30, 30, 30, 30, 30, 30, 15, 30, 100, 100, 30, 30, 30, 30, 30, 30]
LOTL1_UNITS = ['reflectance', 'reflectance', 'reflectance', 'reflectance', 'reflectance', 'reflectance', 'reflectance',
               'reflectance', 'reflectance', 'kelvin', 'kelvin', 'Unitless', 'Unitless', 'degrees', 'degrees',
               'degrees', 'degrees']

LOTL1_BAND_METADATA = {LOTL1_BAND_NAMES[i]: dict(sample_type='UINT16',
                                                 units=LOTL1_UNITS[i],
                                                 wavelength=LOTL1_WAVELENGTHS[i],
                                                 resolution=LOTL1_RESOLUTIONS[i],
                                                 # fill_value=0.0
                                                 )
                       for i in range(len(LOTL1_BAND_NAMES))}

LOTL2_BAND_NAMES = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B10', 'BQA', 'QA_RADSAT', 'SR_QA_AEROSOL',
                    'ST_QA', 'ST_TRAD', 'ST_URAD', 'ST_DRAD', 'ST_ATRAN', 'ST_EMIS', 'ST_EMSD', 'ST_CDIST']
LOTL2_WAVELENGTHS = [443, 482, 561.5, 654.5, 865, 1608.5, 2200.5, 10895, float('nan'), float('nan'), float('nan'),
                     float('nan'), float('nan'), float('nan'), float('nan'), float('nan'), float('nan'), float('nan'),
                     float('nan')]
LOTL2_RESOLUTIONS = [30, 30, 30, 30, 30, 30, 30, 100, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30]
LOTL2_UNITS = ['reflectance', 'reflectance', 'reflectance', 'reflectance', 'reflectance', 'reflectance', 'reflectance',
               'kelvin', 'Unitless', 'Unitless', 'Unitless', 'kelvin', 'radiance', 'radiance', 'radiance', 'Unitless',
               'emissivity coefficient', 'emissivity coefficient', 'kilometers']


LOTL2_BAND_METADATA = {LOTL2_BAND_NAMES[i]: dict(sample_type='UINT16',
                                                 units=LOTL2_UNITS[i],
                                                 wavelength=LOTL2_WAVELENGTHS[i],
                                                 resolution=LOTL2_RESOLUTIONS[i],
                                                 # fill_value=0.0
                                                 )
                       for i in range(len(LOTL2_BAND_NAMES))}

DEM_BAND_METADATA = {'DEM': dict(sample_type='FLOAT32',
                                 units='Meters',
                                 # fill_value=0.0
                                 )
                     }

MODIS_BAND_NAMES = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07']
MODIS_WAVELENGTHS = [645, 858.5, 469, 555, 1240, 1640, 2130]
MODIS_BANDWIDTHS = [50, 35, 20, 20, 20, 24, 50]
MODIS_RESOLUTIONS = [500, 500, 500, 500, 500, 500, 500]
MODIS_BAND_METADATA = {MODIS_BAND_NAMES[i]: dict(sample_type='UINT16',
                                                 units='reflectance',
                                                 wavelength=MODIS_WAVELENGTHS[i],
                                                 bandwidth=MODIS_BANDWIDTHS[i],
                                                 resolution=MODIS_RESOLUTIONS[i],
                                                 # fill_value=0.0
                                                 )
                       for i in range(len(MODIS_BAND_NAMES))}

_SH_METADATA = dict(
    datasets={
        'S1GRD': dict(
            title='Sentinel-1 GRD',
            bands=S1GRD_BAND_METADATA,
            processing_level='L1B',
            request_period='1D',
            collection_name='sentinel-1-grd',
        ),
        'S2L1C': dict(
            title='Sentinel-2 MSI L1C',
            bands=S2L1C_BAND_METADATA,
            processing_level='L1C',
            request_period='1D',
            collection_name='sentinel-2-l1c',
        ),
        'S2L2A': dict(
            title='Sentinel-2 MSI L2A',
            bands=S2L2A_BAND_METADATA,
            processing_level='L2A',
            request_period='1D',
            collection_name='sentinel-2-l2a',
        ),
        'S3OLCI': dict(
            title='Sentinel-3 OCLI L1B',
            bands=S3OLCI_BAND_METADATA,
            processing_level='L1B',
            request_period='1D',
            collection_name='sentinel-3-olci',
        ),
        'S3SLSTR': dict(
            title='Sentinel-3 SLSTR L1B',
            bands=S3SLSTR_BAND_METADATA,
            processing_level='L1B',
            request_period='1D',
            collection_name='sentinel-3-slstr',
        ),
        'S5PL2': dict(
            title='Sentinel-5P - L2',
            bands=S5PL2_BAND_METADATA,
            collection_name='sentinel-5p-l2',
        ),
        'L8L1C': dict(
            title='Landsat 8 - L1C',
            bands=L8L1C_BAND_METADATA,
            processing_level='L1C',
            request_period='1D',
            collection_name='landsat-8-l1c',
        ),
        'LOTL1': dict(
            title='Landsat 8 - L1C',
            bands=LOTL1_BAND_METADATA,
            processing_level='L1C',
            request_period='1D',
            collection_name='landsat-ot-l1',
        ),
        'LOTL2': dict(
            title='Landsat 8 - L2A',
            bands=LOTL2_BAND_METADATA,
            processing_level='L2A',
            request_period='1D',
            collection_name='landsat-ot-l2',
        ),
        'DEM': dict(
            title='Mapzen DEM',
            bands=DEM_BAND_METADATA,
            collection_name=_DEM_COLLECTION_NAME,
        ),
        'MODIS': dict(
            title='MODIS MCD43A4',
            bands=MODIS_BAND_METADATA,
            collection_name='modis',
        ),
        'CUSTOM': dict(
            title='Bring Your Own COG',
            bands={
                # This is custom.
            },
        ),
    }
)
