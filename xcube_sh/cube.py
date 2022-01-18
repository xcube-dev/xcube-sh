# The MIT License (MIT)
# Copyright (c) 2022 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from collections.abc import Callable

import xarray as xr
import zarr

from .chunkstore import SentinelHubChunkStore
from .config import CubeConfig
from .sentinelhub import SentinelHub


def open_cube(cube_config: CubeConfig,
              observer: Callable = None,
              trace_store_calls: bool = False,
              max_cache_size: int = 2 ** 30,
              sentinel_hub: SentinelHub = None,
              **sh_kwargs) -> xr.Dataset:
    """
    Open a data cube from SentinelHub.

    This is a facade function that hides the details of opening a
    volatile data cube from SentinelHub.

    :param cube_config: The cube configuration.
    :param observer: A observer function or callable that is
        called on every request made to SentinelHub.
    :param trace_store_calls: Whether to trace and dump calls
        made into the Zarr store.
    :param max_cache_size: Cache size in bytes. Defaults to 1 GB.
        If zero or None, no caching takes place:
    :param sentinel_hub: Optional instance of SentinelHub,
        the object representing the Sentinel Hub API.
    :param sh_kwargs: Optional keyword arguments passed to the
        SentinelHub constructor. Only valid if
         *sentinel_hub* is not given.
    :return: the data cube represented by an xarray Dataset object.
    """
    if sentinel_hub is None:
        sentinel_hub = SentinelHub(**sh_kwargs)
    elif sh_kwargs:
        raise ValueError(f'unexpected keyword-arguments:'
                         f' {", ".join(sh_kwargs.keys())}')
    cube_store = SentinelHubChunkStore(sentinel_hub,
                                       cube_config,
                                       observer=observer,
                                       trace_store_calls=trace_store_calls)
    if max_cache_size:
        cube_store = zarr.LRUStoreCache(cube_store, max_cache_size)
    return xr.open_zarr(cube_store)
