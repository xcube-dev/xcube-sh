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
