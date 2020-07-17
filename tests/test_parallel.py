import unittest
import pandas

from luigi_report_utils import parallel


class TestParallel(unittest.TestCase):
    def test_df_apply(self):
        df = pandas.DataFrame({"ID": range(10), "VAL": range(100, 110)})

        def _process_row(row):
            row["VAL"] -= 100

        df = parallel.df_apply(df, _process_row)

        pandas.testing.assert_frame_equal(
            pandas.DataFrame({"ID": range(10), "VAL": range(10)}),
            df,
        )


if __name__ == "__main__":
    unittest.main()
