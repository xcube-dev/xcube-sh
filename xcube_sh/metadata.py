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

"""
Static SH metadata.
"""

from typing import Dict, List, Optional, Union


class SentinelHubMetadata:

    def __init__(self):
        self._metadata = _SH_METADATA

    @property
    def datasets(self) -> Dict:
        return dict(self._metadata['datasets'])

    @property
    def dataset_names(self) -> List[str]:
        return [ds_id for ds_id in self._metadata['datasets']]

    def dataset(self, dataset_name: str) -> Optional[Dict]:
        return dict(self._dataset_direct(dataset_name))

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


S2_BAND_NAMES = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B09', 'B10', 'B11', 'B12']
S2A_WAVELENGTHS = [442.7, 492.4, 559.8, 664.6, 704.1, 740.5, 782.8, 832.8, 864.7, 945.1, 1373.5, 1613.7, 2202.4]
S2A_BANDWIDTHS = [21, 66, 36, 31, 15, 15, 20, 106, 21, 20, 31, 91, 175]
S2B_WAVELENGTHS = [442.2, 492.1, 559, 664.9, 703.8, 739.1, 779.7, 832.9, 864, 943.2, 1376.9, 1610.4, 2185.7]
S2B_BANDWIDTHS = [21, 66, 36, 31, 16, 15, 20, 106, 22, 21, 30, 94, 185]
S2_RESOLUTIONS = [60, 10, 10, 10, 20, 20, 20, 10, 20, 60, 60, 20, 20]

S2_BAND_METADATA = {S2_BAND_NAMES[i]: dict(sample_type='FLOAT32',
                                           units='reflectance',
                                           wavelength=S2A_WAVELENGTHS[i],
                                           bandwith=S2A_BANDWIDTHS[i],
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

_SH_METADATA = dict(
    datasets={
        'S1GRD': dict(
            title='Sentinel-1 GRD',
            bands={
                # TODO (forman): add static S1GRD bands metadata here...
            },
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
            bands={
                # TODO (forman): add static S3OCLI bands metadata here...
            },
            processing_level='L1B',
            request_period='1D',
            collection_name='sentinel-3-olci',
        ),
        'S3SLSTR': dict(
            title='Sentinel-3 SLSTR L1B',
            bands={
                # TODO (forman): add static S3SLSTR bands metadata here...
            },
            processing_level='L1B',
            request_period='1D',
            collection_name='sentinel-3-slstr',
        ),
        'S5PL2': dict(
            title='Sentinel-5P - L2',
            bands={
                # TODO (forman): add static DEM bands metadata here...
            },
            collection_name='sentinel-5p-l2',
        ),
        'L8L1C': dict(
            title='Landsat 8 - L1C',
            bands={
                # TODO (forman): add static L8L1C bands metadata here...
            },
            processing_level='L1C',
            request_period='1D',
            collection_name='landsat-8-l1c',
        ),
        'DEM': dict(
            title='Mapzen DEM',
            bands={
                # TODO (forman): add static DEM bands metadata here...
            },
        ),
        'MODIS': dict(
            title='MODIS MCD43A4',
            bands={
                # TODO (forman): add static DEM bands metadata here...
            },
            collection_name='modis',
        ),
        'CUSTOM': dict(
            title='Bring Your Own COG',
            bands={
                # TODO (forman): add static DEM bands metadata here...
            },
        ),
    }
)
