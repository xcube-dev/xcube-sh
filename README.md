# xcube-sh

An xcube extension that creates data cubes from the SentinelHub EO data service.


## Setup

### Install xcube

While xcube is not available from conda-forge, install from sources:

    $ git clone https://github.com/dcs4cop/xcube.git
    $ cd xcube
    $ conda env create
    $ conda activate xcube
    $ python setup.py develop
    
When xcube will be available from conda-forge:
    
    $ conda create --name xcube xcube>0.3
    $ conda activate xcube
    
### Install xcube-sh

Update conda environment to satisfy xcube-sh package requirements:

    $ conda env update
    
Or manually:
    
    $ conda install -c conda-forge oauthlib pip
    $ pip install requests_oauthlib


Install xcube-sh from sources:

    $ git clone https://gitext.sinergise.com/dcfs/xcube-sh.git
    $ cd xcube-sh
    $ conda activate xcube
    $ python setup.py develop

Setup SentinelHub credentials:

    $ export SH_INSTANCE_ID=<your instance ID>    
    $ export SH_CLIENT_ID=<your client ID>    
    $ export SH_CLIENT_SECRET=<your client secret>    

    
Test:

    $ pytest

    
## Tools

Check available XCube/SentinelHub integration tools:

    $ xcsh --help
    $ xcsh meta --help
    $ xcsh cubify --help

### Generate a Cube: `xcsh cubify` 

Generate an XCube-compatible ZARR data cube from Sentinel-2 L1C data:

    $ xcsh meta
    $ xcsh meta S2L1C
    $ xcsh cubify S2L1C -o cube.zarr -b B08 -g 10.237174,53.506205,10.271174,53.540205 -r 6.640625e-05 -t 2017-08-01,2017-08-31 -p 1D
    $ python
    >>> import xarray as xr
    >>> cube = xr.open_zarr('cube.zarr')
    >>> cube
    >>> cube.B08
    >>> cube.B08.isel(time=22).plot.imshow()
