# The MIT License (MIT)
# Copyright (c) 2020 by the xcube development team and contributors
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

import json
import unittest
from collections import MutableMapping
from typing import Dict, KeysView, Iterator

import numpy as np
import xarray


class ReproduceIssue27Test(unittest.TestCase):

    def test_reproduce_issue_27(self):
        values = [12, 13, 14]
        array = np.array(values)
        shape = list(map(int, array.shape))
        dtype = str(array.dtype.str)
        order = "C"

        zgroup = {
            "zarr_format": 2,
        }

        zattrs = {
        }

        lon_zarray = {
            "zarr_format": 2,
            "chunks": shape,
            "shape": shape,
            "dtype": dtype,
            "order": order,
            "fill_value": None,
            "compressor": None,
            "filters": None,
        }

        lon_zattrs = {
            "_ARRAY_DIMENSIONS": ["lon"]
        }

        src_store = WritableStoreThatIsNotADict({
            '.zgroup': bytes(json.dumps(zgroup, indent=2), encoding='utf-8'),
            '.zattrs': bytes(json.dumps(zattrs, indent=2), encoding='utf-8'),
            'lon/.zarray': bytes(json.dumps(lon_zarray, indent=2), encoding='utf-8'),
            'lon/.zattrs': bytes(json.dumps(lon_zattrs, indent=2), encoding='utf-8'),
            'lon/0': array.tobytes(order=order)
        })
        self.assertEqual({'.zgroup', '.zattrs', 'lon/.zarray', 'lon/.zattrs', 'lon/0'},
                         set(src_store.keys()))

        dataset = xarray.open_zarr(src_store)
        self.assertIn('lon', dataset)
        self.assertEqual(values, list(dataset.lon.values))

        dst_store = WritableStoreThatIsNotADict()
        dataset.to_zarr(dst_store)

        self.assertEqual(set(src_store.keys()), set(dst_store.keys()))

        self.assertIsInstance(src_store['lon/0'], bytes)
        with self.assertRaises(AssertionError) as cm:
            self.assertIsInstance(dst_store['lon/0'], bytes)
        self.assertEqual("array([12, 13, 14]) is not an instance of <class 'bytes'>",
                         f'{cm.exception}')


class WritableStoreThatIsNotADict(MutableMapping):
    """
    A MutableMapping that is NOT a dict.
    All interface operations are delegated to a wrapped dict instance.
    :param entries: optional entries
    """

    def __init__(self, entries: Dict[str, bytes] = None):
        self._entries: Dict[str, bytes] = dict(entries) if entries else dict()

    def keys(self) -> KeysView[str]:
        return self._entries.keys()

    def __iter__(self) -> Iterator[str]:
        return iter(self._entries.keys())

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, key) -> bool:
        return key in self._entries

    def __getitem__(self, key: str) -> bytes:
        return self._entries[key]

    def __setitem__(self, key: str, value: bytes) -> None:
        self._entries[key] = value

    def __delitem__(self, key: str) -> None:
        del self._entries[key]
