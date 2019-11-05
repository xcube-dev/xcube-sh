# xcube-sh

An [xcube plugin]() that allows generating data cubes from the SENTINEL Hub Cloud API.

## Setup

First install [`xcube`](https://github.com/dcs4cop/xcube), then the `xcube_sh` plugin.

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

You cannot use the `xcube_sh` plugin without specifying your SENTINEL Hub credentials:

    $ export SH_INSTANCE_ID=<your instance ID>    
    $ export SH_CLIENT_ID=<your client ID>    
    $ export SH_CLIENT_SECRET=<your client secret>    

While `xcube_sh` is not yet available from conda-forge, install it from sources. 
We'll need to update the `xcube` environment first, then install `xcube_sh`:

    $ conda activate xcube
    $ conda install -c conda-forge oauthlib pip
    $ pip install requests_oauthlib
    
    $ git clone https://github.com/dcs4cop/xcube-sh.git
    $ cd xcube-sh
    $ python setup.py develop

Once `xcube_sh` will be available from conda-forge:

    $ conda activate xcube
    $ conda install -c conda-forge xcube_sh

### Test:

    $ pytest

    
## Tools

Check available xcube CLI extensions added by `xcube_sh` plugin:

    $ xcube sh --help
    $ xcube sh gen --help
    $ xcube sh info --help

### Generate a Cube: `xcube sh gen` 

Generate an XCube-compatible ZARR data cube from Sentinel-2 L1C data:

    $ xcube sh info
    $ xcube sh info S2L1C
    $ xcube sh gen S2L1C -o cube.zarr -b B08 -g 10.237174,53.506205,10.271174,53.540205 -r 6.640625e-05 -t 2017-08-01,2017-08-31 -p 1D
    $ python
    >>> import xarray as xr
    >>> cube = xr.open_zarr('cube.zarr')
    >>> cube
    >>> cube.B08
    >>> cube.B08.isel(time=22).plot.imshow()
