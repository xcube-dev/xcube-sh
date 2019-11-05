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
