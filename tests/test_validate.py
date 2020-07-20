import unittest
import pandas

from luigi_report_utils import validate


class TestXrefIntegrity(unittest.TestCase):
    def test_full(self):
        """create two tables where there is 1 column in each that are identical
        then assert those columns pass the xref check."""
        df1 = pandas.DataFrame({"A": range(10), "B": range(10, 10 + 10),})
        df2 = pandas.DataFrame({"C": range(10, 10 + 10), "D": range(10, 0, -1),})

        failed_records = list(validate.xref_integrity(df1, "B", df2, "C"))
        self.assertEqual(failed_records, [])

    def test_partial(self):
        """create two tables where there is 1 column is shared but the second
        table has half as many rows, then assert that the xref check passes."""
        df1 = pandas.DataFrame({"A": range(10), "B": range(10, 10 + 10),})
        df2 = pandas.DataFrame(
            {"C": range(10, 10 + 10, 2), "D": range(5),}  # Skip every other key
        )

        failed_records = list(validate.xref_integrity(df1, "B", df2, "C"))
        self.assertEqual(len(failed_records), 5)


class TestUniqueKeys(unittest.TestCase):
    def test_basic(self):
        df = pandas.DataFrame({"A": range(10), "B": range(100,110),})
        failed_rows = list(validate.unique_keys(df, ["A"]))
        self.assertEqual(failed_rows, [])

    def test_basic_fail(self):
        df = pandas.DataFrame({"A": range(5), "B": range(100,105),})
        df = df.append(df.copy(), ignore_index=True)
        failed_rows = list(validate.unique_keys(df, ["A"]))
        self.assertEqual(len(failed_rows), 10)

    def test_multi_key(self):
        df = pandas.DataFrame({"A": range(5), "B": range(100,105),})
        df = df.append(
            pandas.DataFrame({"A": range(5), "B": range(105,110),}),
            ignore_index=True)
        failed_rows = list(validate.unique_keys(df, ["A", "B"]))
        self.assertEqual(failed_rows, [])
