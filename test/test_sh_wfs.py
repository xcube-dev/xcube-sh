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

import os
import unittest

from xcube_sh import SentinelHub

HAS_SH_INSTANCE_ID = 'SH_INSTANCE_ID' in os.environ
REQUIRE_SH_INSTANCE_ID = 'requires SH instance ID'


@unittest.skipUnless(HAS_SH_INSTANCE_ID, REQUIRE_SH_INSTANCE_ID)
class ShWfsTest(unittest.TestCase):

    def test_fetch_tiles(self):
        instance_id = os.environ.get('SH_INSTANCE_ID')

        x1 = 10.00  # degree
        y1 = 54.27  # degree
        x2 = 11.00  # degree
        y2 = 54.60  # degree

        t1 = '2019-09-17'
        t2 = '2019-10-17'

        tile_features = SentinelHub.fetch_tile_features(instance_id=instance_id,
                                                        feature_type_name='S2.TILE',
                                                        bbox=(x1, y1, x2, y2),
                                                        time_range=(t1, t2))

        self.assertEqual(32, len(tile_features))

        for feature in tile_features:
            self.assertEqual('Feature', feature.get('type'))
            self.assertIn('geometry', feature)
            self.assertIn('properties', feature)
            properties = feature['properties']
            self.assertIn('id', properties)
            self.assertIn('path', properties)
            self.assertIn('date', properties)
            self.assertIn('time', properties)
