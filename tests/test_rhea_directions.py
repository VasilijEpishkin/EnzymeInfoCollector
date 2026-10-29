"""Offline Rhea direction mapping tests."""

import unittest

from enzymeinfocollector.domain import ReactionDirection
from enzymeinfocollector.rhea_directions import parse_rhea_directions
from enzymeinfocollector.sources import DataSourceError

HEADER = "RHEA_ID_MASTER\tRHEA_ID_LR\tRHEA_ID_RL\tRHEA_ID_BI\n"


class RheaDirectionsTests(unittest.TestCase):
    def test_all_four_variants_have_one_master(self):
        lookup = parse_rhea_directions(HEADER + "10000\t10001\t10002\t10003\n")
        self.assertEqual(len(lookup), 4)
        self.assertEqual(lookup["RHEA:10000"], ("RHEA:10000", ReactionDirection.UNDEFINED))
        self.assertEqual(lookup["RHEA:10001"], ("RHEA:10000", ReactionDirection.LEFT_TO_RIGHT))
        self.assertEqual(lookup["RHEA:10002"], ("RHEA:10000", ReactionDirection.RIGHT_TO_LEFT))
        self.assertEqual(lookup["RHEA:10003"], ("RHEA:10000", ReactionDirection.BIDIRECTIONAL))

    def test_invalid_rows_are_rejected(self):
        for data in (
            "WRONG\tRHEA_ID_LR\tRHEA_ID_RL\tRHEA_ID_BI\n10000\t10001\t10002\t10003\n",
            HEADER,
            HEADER + "10000\t10001\t10002\n",
            HEADER + "10000\t10001\t10001\t10003\n",
            HEADER + "10000\t0\t10002\t10003\n",
            HEADER + "10000\t10001\t10002\t10003\textra\n",
        ):
            with self.subTest(data=data), self.assertRaises(DataSourceError):
                parse_rhea_directions(data)


if __name__ == "__main__":
    unittest.main()
