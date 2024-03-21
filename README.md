[![Build Status](https://ci.appveyor.com/api/projects/status/0n1boma6tdt4qhta/branch/main?svg=true)](https://ci.appveyor.com/project/bcdev/xcube-sh)
[![Conda Version](https://anaconda.org/conda-forge/xcube-sh/badges/version.svg)](https://anaconda.org/conda-forge/xcube-sh)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License](https://img.shields.io/github/license/dcs4cop/xcube-sh)](https://github.com/dcs4cop/xcube-sh)


# xcube-sh - xcube Data Store for Sentinel Hub

An [xcube plugin](https://xcube.readthedocs.io/en/latest/plugins.html) for
generating data cubes from [Sentinel Hub](https://www.sentinel-hub.com/).

## Setup

First install [`xcube`](https://github.com/dcs4cop/xcube), 
then the `xcube_sh` plugin.

### Install xcube

While `xcube` is not yet available from conda-forge, install from sources:

    $ git clone https://github.com/dcs4cop/xcube.git
    $ cd xcube
    $ conda env create
    $ conda activate xcube
    $ python setup.py develop
    
Once `xcube` will be available from conda-forge:
    
    $ conda create --name xcube xcube>=0.3
    $ conda activate xcube
    
### Install xcube_sh

You cannot use the `xcube_sh` plugin without specifying your Sentinel Hub 
credentials:

    $ export SH_CLIENT_ID=<your client ID>    
    $ export SH_CLIENT_SECRET=<your client secret>    

While `xcube_sh` is not yet available from conda-forge, 
install it from sources. We'll need to update the `xcube` environment first, 
then install `xcube_sh`:

    $ conda activate xcube
    $ conda install -c conda-forge oauthlib pip
    $ pip install requests_oauthlib
    
    $ git clone https://github.com/dcs4cop/xcube-sh.git
    $ cd xcube-sh
    $ python setup.py develop

Once `xcube_sh` will be available from conda-forge:

    $ conda activate xcube
    $ conda install -c conda-forge xcube-sh

### Test:

    $ pytest

    
## Tools

Check available xcube CLI extensions added by `xcube_sh` plugin:

    $ xcube sh --help
    $ xcube sh gen --help
    $ xcube sh info --help

### Generate a Cube: `xcube sh gen` 

Generate an xcube-compatible ZARR data cube from Sentinel-2 L1C data:

    $ xcube sh info
    $ xcube sh info S2L1C
    $ xcube sh gen S2L1C -o cube.zarr -b B08 -g 10.237174,53.506205,10.271174,53.540205 -r 6.640625e-05 -t 2017-08-01,2017-08-31 -p 1D
    $ python
    >>> import xarray as xr
    >>> cube = xr.open_zarr('cube.zarr')
    >>> cube
    >>> cube.B08
    >>> cube.B08.isel(time=22).plot.imshow()
