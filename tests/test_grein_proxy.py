import unittest
import os

from grein_proxy.flask_app import create_app


class TestGreinProxy(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()

        # set the path to the test database
        db_path = os.path.dirname(__file__) + "/grein_proxy.db"
        os.environ["GREIN_DB"] = db_path

        # create the test client
        self.client = create_app().test_client()

    def testStatus(self):
        res = self.client.get("/GSE100040/status")

        self.assertEqual(200, res.status_code)

        # emsure that it's a valid JSON response
        data = res.json

        self.assertEqual("GSE100040", data["accession"])
        self.assertEqual(1, data["status"])

        # test a status 0 dataset
        res0 = self.client.get("/TEST/status")

        self.assertEqual(200, res0.status_code)

        # emsure that it's a valid JSON response
        data0 = res0.json

        self.assertEqual("TEST", data0["accession"])
        self.assertEqual(0, data0["status"])

        # test an unknown dataset
        res_missing = self.client.get("/NO_ID/status")

        self.assertEqual(200, res_missing.status_code)

        self.assertEqual("Unknown", res_missing.json["status"])

    def testMetadata(self):
        res = self.client.get("/GSE100040/metadata.json")

        self.assertEqual(200, res.status_code)
        metadata = res.json

        self.assertTrue("GSM2671001" in metadata)

        # test for missing data
        res_missing = self.client.get("/TEST/metadata.json")
        self.assertEqual(404, res_missing.status_code)

        # test for missing data
        res_missing = self.client.get("/NOTHING/metadata.json")
        self.assertEqual(404, res_missing.status_code)

    def testCounts(self):
        res = self.client.get("/GSE100040/raw_counts.tsv")

        self.assertEqual(200, res.status_code)
        raw_counts = res.data.decode()

        self.assertIsNotNone(raw_counts)

        # test for missing data
        res_missing = self.client.get("/TEST/raw_counts.tsv")
        self.assertEqual(404, res_missing.status_code)

        # test for missing data
        res_missing = self.client.get("/NOTHING/raw_counts.tsv")
        self.assertEqual(404, res_missing.status_code)