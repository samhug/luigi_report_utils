import unittest
import pandas

from luigi_report_utils import parallel

_SIZE = 1000


class TestParallel(unittest.TestCase):
    def test_df_apply_basic(self):
        df = pandas.DataFrame({"A": range(_SIZE), "B": range(_SIZE)})

        def _process_row(row):
            row["B"] += 1

        df_r = parallel.df_apply(df, _process_row)

        # Verify that the return value is the original dataframe
        self.assertTrue(df is df_r)

        # Verify results
        df_expected = pandas.DataFrame({"A": range(_SIZE), "B": range(1, _SIZE + 1)})
        pandas.testing.assert_frame_equal(df_expected, df)

    def test_parallel_apply_keyerror(self):
        df = pandas.DataFrame({"A": range(_SIZE), "B": range(_SIZE)})

        def _process_row(row):
            row["C"] += 1

        with self.assertRaises(KeyError):
            parallel.df_apply(df, _process_row)

    def test_parallel_apply_2(self):
        df = pandas.DataFrame(
            {"A": range(_SIZE), "B": range(1, _SIZE + 1), "C": [0] * _SIZE}
        )

        def _process_row(row):
            row["C"] = row["A"] + row["B"]

        parallel.df_apply(df, _process_row)

        # Verify results
        for row in df.itertuples():
            self.assertEqual(row.C, (row.A + row.B))

    def test_parallel_apply_rowproxy_default(self):
        df = pandas.DataFrame({"A": range(_SIZE)})
        df = df.assign(C=0)

        def _process_row(row):
            row["C"] = row.get("B", row["A"])

        parallel.df_apply(df, _process_row)

        # Verify results
        df_expected = pandas.DataFrame({"A": range(_SIZE), "C": range(_SIZE)})
        pandas.testing.assert_frame_equal(df_expected, df)

    def test_parallel_apply_rowproxy_dict(self):
        df = pandas.DataFrame({"A": range(_SIZE)})
        df = df.assign(B="")

        def _process_row(row):
            d = row.dict()
            row["B"] = str(d)

        parallel.df_apply(df, _process_row)

        # Verify results
        df_expected = pandas.DataFrame(
            {"A": range(_SIZE), "B": [str({"A": i, "B": ""}) for i in range(_SIZE)]}
        )
        pandas.testing.assert_frame_equal(df_expected, df)

    def test_parallel_apply_return_results(self):
        df = pandas.DataFrame({"A": range(_SIZE), "B": range(1, _SIZE + 1)})

        def _process_row(row):
            row["A"] += 1
            return ("1", row["A"], row["B"])

        results = parallel.df_apply(df, _process_row, return_df=False)

        # Verify results
        df_expected = pandas.DataFrame(
            {"A": range(1, _SIZE + 1), "B": range(1, _SIZE + 1)}
        )
        pandas.testing.assert_frame_equal(df_expected, df)

        df = pandas.DataFrame(results)
        df_expected = pandas.DataFrame(
            {0: ["1"] * _SIZE, 1: range(1, _SIZE + 1), 2: range(1, _SIZE + 1)}
        )
        pandas.testing.assert_frame_equal(df_expected, df)


if __name__ == "__main__":
    unittest.main()
