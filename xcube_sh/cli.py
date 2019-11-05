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

import sys

import click

from xcube_sh.constants import DEFAULT_CRS, DEFAULT_TIME_TOLERANCE
from xcube_sh.version import version

DEFAULT_OUTPUT = 'out.zarr'


@click.command(name="gen",
               # Required for parsing geometry bbox with negative coordinates
               context_settings={"ignore_unknown_options": True})
@click.argument('dataset')
@click.option('--output', '-o', 'output_path',
              help=f'Output ZARR directory. Defaults to "{DEFAULT_OUTPUT}".',
              default=DEFAULT_OUTPUT)
@click.option('--band', '-b', 'band_names',
              help='Band name. Can be repeated for multiple bands. Defaults to all bands of a dataset.',
              multiple=True)
@click.option('--tile_size', '--tile-size',
              help='Tile size given as number of grid cells using format "<size>" or "<width>,<height>".')
@click.option('--geom', '-g', 'geometry',
              help='Geometry WKT, GeoJSON object, or bounding box using format "<lon1>,<lat1>,<lon2>,<lat2>". '
                   'Coordinates must be in decimal degree.',
              required=True)
@click.option('--res', '-r', 'spatial_res',
              help="Spatial resolution in degrees.",
              type=float,
              required=True)
@click.option('--crs',
              help=f'Coordinate reference system (CRS) URL. Defaults to "{DEFAULT_CRS}".',
              default=DEFAULT_CRS)
@click.option('--time', '-t', 'time_range',
              help='Time or time range using format "date" or "<first-date>,<last-date>".',
              required=True)
@click.option('--period', '-p', 'time_period',
              help='Time (aggregation) period. Format is "<period>" or "<num><period>" '
                   'where <num> is a positive integer and <period> is one of "H", "D", "W", "Y". '
                   'Defaults to a period suitable for the dataset.')
@click.option('--tolerance', '--tol', 'time_tolerance',
              default=DEFAULT_TIME_TOLERANCE,
              help='Time (request) tolerance. Format is "<period>" or "<num><period>" '
                   'where <num> is a positive integer and <period> is one of "S", "M", "H". '
                   f'Defaults to "{DEFAULT_TIME_TOLERANCE}".')
@click.option('--4d', 'four_d',
              is_flag=True,
              help='Write a single data 4D array "band_data" to the output. '
                   'Will slightly increase execution speed. '
                   'By default, bands are written to separate 3D arrays, e.g. "B01", "B02".')
@click.option('--verbose', '-v',
              is_flag=True,
              help='Print information about each single SH request to stdout.')
def gen(dataset,
        output_path,
        band_names,
        tile_size,
        geometry,
        spatial_res,
        crs,
        time_range,
        time_period,
        time_tolerance,
        four_d,
        verbose):
    """
    Generate a data cube from SentinelHub.

    By default, the command will create a ZARR dataset with 3D arrays
    for each band e.g. "B01", "B02" with dimensions "time", "lat", "lon".
    Use option "--4d" to write a single 4D array "band_data"
    with dimensions "time", "lat", "lon", "band".
    """
    import os.path
    import time
    import xarray as xr
    from xcube_sh.config import CubeConfig
    from xcube_sh.observers import Observers
    from xcube_sh.sentinelhub import SentinelHub
    from xcube_sh.store import SentinelHubStore

    if os.path.exists(output_path):
        raise click.ClickException(f'Output {output_path} already exists. Move it away first.')

    cube_config = CubeConfig(dataset_name=dataset,
                             band_names=band_names,
                             chunk_size=tile_size,
                             geometry=geometry,
                             spatial_res=spatial_res,
                             crs=crs,
                             time_range=time_range,
                             time_period=time_period,
                             time_tolerance=time_tolerance,
                             four_d=four_d,
                             exception_type=click.ClickException)

    sentinel_hub = SentinelHub()

    print(f'Writing cube to {output_path}...')

    t0 = time.perf_counter()
    store = SentinelHubStore(sentinel_hub, cube_config)
    request_collector = Observers.request_collector()
    store.add_observer(request_collector)
    if verbose:
        store.add_observer(Observers.request_dumper())
    cube = xr.open_zarr(store)
    cube.to_zarr(output_path)
    duration = time.perf_counter() - t0

    print(f"Cube written to {output_path}, took {'%.2f' % duration} seconds.")

    if verbose:
        request_collector.stats.dump()


@click.command(name="info")
@click.argument('datasets', nargs=-1)
def info(datasets=None):
    """
    Print SentinelHub metadata info. If DATASETS (names of datasets) are not present,
    the list of available dataset names are returned. Otherwise,
    the the variables of the given datasets are returned.
    """
    from xcube_sh.sentinelhub import SentinelHub

    sentinel_hub = SentinelHub()
    import json
    if not datasets:
        response = dict(datasets=sentinel_hub.dataset_names)
    else:
        response = dict()
        for dataset_name in datasets:
            band_names = sentinel_hub.band_names(dataset_name)
            bands = dict()
            for band_name in band_names:
                bands[band_name] = sentinel_hub.METADATA.dataset_band(dataset_name, band_name, default={})
            response[dataset_name] = bands
    print(json.dumps(response, indent=2))


# noinspection PyShadowingBuiltins,PyUnusedLocal
@click.group(name="sh")
@click.version_option(version)
def cli():
    """
    SentinelHub tools for xcube.
    """


cli.add_command(gen)
cli.add_command(info)


def main(args=None):
    # noinspection PyBroadException
    try:
        exit_code = cli.main(args=args, standalone_mode=False)
    except click.ClickException as e:
        e.show()
        exit_code = 1
    except Exception as e:
        import traceback
        traceback.print_exc()
        exit_code = 2
        print(f'Error: {e}')
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
