from collections import Callable

import xarray as xr
import zarr

from .config import CubeConfig
from .sentinelhub import SentinelHub
from .store import SentinelHubStore


def open_cube(cube_config: CubeConfig,
              observer: Callable = None,
              trace_store_calls: bool = False,
              max_cache_size: int = 2 ** 30,
              **sh_kwargs) -> xr.Dataset:
    """
    Open a data cube from SentinelHub.

    This is a facade function that hides the details of opening a volatile data cube from SentinelHub.

    :param cube_config: The cube configuration.
    :param observer: A observer function or callable that is called on every request made to SentinelHub.
    :param trace_store_calls: Whether to trace and dump calls made into the Zarr store.
    :param max_cache_size: Cache size in bytes. Defaults to 1 GB. If zero or None, no caching takes place:
    :param sh_kwargs: Keyword arguments passed to the SentinelHub constructor.
    :return: the data cube represented by an xarray Dataset object.
    """
    sentinel_hub = SentinelHub(**sh_kwargs)
    cube_store = SentinelHubStore(sentinel_hub, cube_config, observer=observer, trace_store_calls=trace_store_calls)
    if max_cache_size:
        cube_store = zarr.LRUStoreCache(cube_store, max_cache_size)
    return xr.open_zarr(cube_store)
