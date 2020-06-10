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

import os.path
import unittest

from xcube.core.store.dataaccess import get_data_accessor_class
from xcube.core.store.dataaccess import get_data_accessor_infos
from xcube.core.store.dataaccess import new_data_accessor
from xcube.util.jsonschema import JsonObjectSchema
from xcube_sh.dataaccess import SentinelHubDataAccessor
from .test_sentinelhub import HAS_SH_CREDENTIALS
from .test_sentinelhub import REQUIRE_SH_CREDENTIALS

THIS_DIR = os.path.dirname(__file__)


class SentinelHubDataAccessorTest(unittest.TestCase):

    def test_data_accessor_infos(self):
        data_accessor_infos = get_data_accessor_infos()
        self.assertIn('sentinelhub', data_accessor_infos)

    def test_data_accessor_class(self):
        data_accessor_class = get_data_accessor_class('sentinelhub')
        self.assertIsInstance(data_accessor_class(), SentinelHubDataAccessor)
        schema = data_accessor_class.get_data_accessor_params_schema()
        self.assertIsInstance(schema, JsonObjectSchema)

    @unittest.skipUnless(HAS_SH_CREDENTIALS, REQUIRE_SH_CREDENTIALS)
    def test_dataset_opener(self):
        data_accessor = new_data_accessor('sentinelhub')
        self.assertIsInstance(data_accessor, SentinelHubDataAccessor)
