import json
import os.path
from typing import Any, Dict, Sequence, Union

import numpy as np
import zarr


class ZarrWriter:

    def __init__(self, root_path: str):
        self._root_path = root_path
        self._ensured_paths = set()

    def ensure_dir(self, dir_path):
        if dir_path not in self._ensured_paths and not os.path.exists(dir_path):
            os.mkdir(dir_path)
            self._ensured_paths.add(dir_path)

    def ensure_root_dir(self):
        self.ensure_dir(self._root_path)

    def ensure_sub_dir(self, name: str):
        self.ensure_root_dir()
        self.ensure_dir(self.sub_path(name))

    def sub_path(self, *names: str) -> str:
        return os.path.join(self._root_path, *names)

    def write_group_metadata(self, attrs: Dict[str, Any] = None):
        self.ensure_root_dir()
        self.write_json(self.sub_path('.zgroup'),
                        dict(zarr_format=2))
        self.write_json(self.sub_path('.zattrs'),
                        attrs or dict())

    def write_array(self,
                    array_name: str,
                    array: Union[np.ndarray, Sequence[Any]],
                    attrs: Dict[str, Any] = None):
        zarr.convenience.save_array(self.sub_path(array_name), array, fill_value=None)
        if attrs:
            array = zarr.convenience.open_array(self.sub_path(array_name), 'r+')
            array.attrs.update(**(attrs or dict()))

    def write_slice_bytes(self,
                          array_name: str,
                          dim_index: int,
                          num_dims: int,
                          slice_index: int,
                          slice_bytes: Any):
        array_index = [0] * num_dims
        array_index[dim_index] = slice_index
        array_file_name = '.'.join(map(str, array_index))
        self.ensure_sub_dir(array_name)
        self.write_byte_data(os.path.join(self.sub_path(array_name), array_file_name), slice_bytes)

    def write_slice_bytes_metadata(self,
                                   array_name: str,
                                   shape: Sequence[int],
                                   chunks: Sequence[int],
                                   dtype: str,
                                   fill_value: Union[None, int, float],
                                   attrs: Dict[str, Any] = None):
        self.ensure_sub_dir(array_name)
        self.write_json(self.sub_path(array_name, '.zarray'),
                        dict(
                            zarr_format=2,
                            shape=shape,
                            chunks=chunks,
                            dtype=dtype,
                            fill_value=fill_value,
                            order="C",
                            filters=None,
                            compressor=dict(id='zlib', level=8),
                        ))
        self.write_json(self.sub_path(array_name, '.zattrs'),
                        attrs or dict())

    @classmethod
    def write_json(cls, file_path: str, obj: Dict[str, Any]):
        with open(file_path, 'w') as fp:
            json.dump(obj, fp, indent=2)

    @classmethod
    def write_byte_data(cls, file_path: str, byte_data: Any):
        with open(file_path, 'wb') as fp:
            fp.write(byte_data)
