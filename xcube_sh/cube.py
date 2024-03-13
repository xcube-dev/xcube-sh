# Copyright © 2022-2024 by the xcube development team and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.


from typing import Callable

import xarray as xr
import zarr

from .chunkstore import SentinelHubChunkStore
from .config import CubeConfig
from .sentinelhub import SentinelHub


def open_cube(
    cube_config: CubeConfig,
    observer: Callable = None,
    trace_store_calls: bool = False,
    max_cache_size: int = 2**30,
    sentinel_hub: SentinelHub = None,
    **sh_kwargs,
) -> xr.Dataset:
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
        raise ValueError(
            f"unexpected keyword-arguments:" f' {", ".join(sh_kwargs.keys())}'
        )
    cube_store = SentinelHubChunkStore(
        sentinel_hub,
        cube_config,
        observer=observer,
        trace_store_calls=trace_store_calls,
    )
    if max_cache_size:
        cube_store = zarr.LRUStoreCache(cube_store, max_cache_size)

    cube = xr.open_zarr(cube_store)
    if hasattr(cube, "zarr_store"):
        cube.zarr_store.set(cube_store)

    return cube
