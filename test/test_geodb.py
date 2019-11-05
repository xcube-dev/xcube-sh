# The MIT License (MIT)
# Copyright (c) 2019 by the xcube development team and contributors
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

import unittest

from xcube_sh.geodb import GeoDB


class GeoDBTest(unittest.TestCase):

    def setUp(self) -> None:
        self.geo_db = GeoDB()

    def test_find_feature(self):
        x = self.geo_db.find_feature('S_NAME == "Lago di Garda"')
        self.assertIsNone(x)

        x = self.geo_db.find_feature('S_NAME == "Selenter_See"')
        self.assertIsNotNone(x)

    def test_find_features(self):
        x = self.geo_db.find_features('S_NAME == "Lago di Garda"')
        self.assertEqual(0, len(x))

        x = self.geo_db.find_features('S_NAME == "Selenter_See"')
        self.assertEqual(1, len(x))
