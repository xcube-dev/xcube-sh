# Copyright © 2022-2024 by the xcube development team and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from xcube.constants import EXTENSION_POINT_CLI_COMMANDS
from xcube.constants import EXTENSION_POINT_DATA_OPENERS
from xcube.constants import EXTENSION_POINT_DATA_STORES
from xcube.util import extension
from xcube_sh.constants import SH_DATA_OPENER_ID
from xcube_sh.constants import SH_DATA_STORE_ID
from xcube_sh.constants import SH_CDSE_DATA_STORE_ID


def init_plugin(ext_registry: extension.ExtensionRegistry):
    # xcube SentinelHub extensions
    ext_registry.add_extension(
        loader=extension.import_component("xcube_sh.main:cli"),
        point=EXTENSION_POINT_CLI_COMMANDS,
        name="sh_cli",
    )

    # xcube DataAccessor extensions
    ext_registry.add_extension(
        loader=extension.import_component("xcube_sh.store:SentinelHubDataStore"),
        point=EXTENSION_POINT_DATA_STORES,
        name=SH_DATA_STORE_ID,
        description="Sentinel Hub Cloud API",
    )

    # xcube DataAccessor extensions
    ext_registry.add_extension(
        loader=extension.import_component("xcube_sh.store:SentinelHubCdseDataStore"),
        point=EXTENSION_POINT_DATA_STORES,
        name=SH_CDSE_DATA_STORE_ID,
        description="Sentinel Hub Cloud API on CDSE",
    )

    # xcube DataAccessor extensions
    ext_registry.add_extension(
        loader=extension.import_component("xcube_sh.store:SentinelHubDataOpener"),
        point=EXTENSION_POINT_DATA_OPENERS,
        name=SH_DATA_OPENER_ID,
        description=(
            "xarray.Dataset cubes in Zarr format " "from Sentinel Hub Cloud API"
        ),
    )
