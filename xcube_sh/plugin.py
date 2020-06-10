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

from xcube.constants import EXTENSION_POINT_CLI_COMMANDS
from xcube.constants import EXTENSION_POINT_DATA_ACCESSORS
from xcube.util import extension


def init_plugin(ext_registry: extension.ExtensionRegistry):
    # xcube SentinelHub extensions
    ext_registry.add_extension(loader=extension.import_component('xcube_sh.main:cli'),
                               point=EXTENSION_POINT_CLI_COMMANDS,
                               name='sh_cli')

    # xcube DataAccessor extensions
    ext_registry.add_extension(loader=extension.import_component('xcube_sh.dataaccess:SentinelHubDataAccessor'),
                               point=EXTENSION_POINT_DATA_ACCESSORS,
                               name='sentinelhub',
                               description='SENTINEL Hub Cloud API')
