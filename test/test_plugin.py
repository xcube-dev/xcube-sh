# Copyright © 2022-2024 by the xcube development team and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

from xcube.util.extension import ExtensionRegistry
from xcube_sh.plugin import init_plugin


class XcubePluginTest(unittest.TestCase):
    def test_plugin(self):
        """Assert xcube extensions registered by xcube-sh"""
        registry = ExtensionRegistry()
        init_plugin(registry)
        self.assertEqual(
            {
                "xcube.cli": {
                    "sh_cli": {
                        "component": "<not loaded yet>",
                        "name": "sh_cli",
                        "point": "xcube.cli",
                    }
                },
                "xcube.core.store": {
                    "sentinelhub": {
                        "component": "<not loaded yet>",
                        "description": "Sentinel Hub Cloud API",
                        "name": "sentinelhub",
                        "point": "xcube.core.store",
                    },
                    "sentinelhub-cdse": {
                        "component": "<not loaded yet>",
                        "description": "Sentinel Hub Cloud API on CDSE",
                        "name": "sentinelhub-cdse",
                        "point": "xcube.core.store",
                    },
                },
                "xcube.core.store.opener": {
                    "dataset:zarr:sentinelhub": {
                        "component": "<not loaded yet>",
                        "description": "xarray.Dataset "
                        "cubes "
                        "in "
                        "Zarr "
                        "format "
                        "from "
                        "Sentinel "
                        "Hub "
                        "Cloud "
                        "API",
                        "name": "dataset:zarr:sentinelhub",
                        "point": "xcube.core.store.opener",
                    }
                },
            },
            registry.to_dict(),
        )
