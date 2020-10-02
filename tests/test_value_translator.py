import pandas
import unittest

from luigi_report_utils import inpt, records, value_translator


class TestVTT(unittest.TestCase):
    def test_from_str(self):
        df = pandas.DataFrame({"A": range(10), "B": range(10)}, dtype=str)

        STR_IN = """old-val,new-val
0,10
1,9
2,8
3,7
4,6"""

        vt = value_translator.ValueTranslator()
        vt.add_vtt("B", value_translator.load_from_csv(inpt.from_str(STR_IN)))
        vt.translate(df)

        pandas.testing.assert_frame_equal(
            pandas.DataFrame(
                {"A": range(10), "B": list(range(10, 5, -1)) + list(range(5, 10))},
                dtype=str,
            ),
            df,
        )

    def test_vtt_strict(self):
        df = pandas.DataFrame({"A": range(10), "B": range(10)}, dtype=str)

        STR_IN = """old-val,new-val
0,10
1,9
2,8
3,7
4,6
5,5
6,4
7,3
8,2
9,1
10,0"""

        vt = value_translator.ValueTranslator()
        vt.add_vtt(
            "B", value_translator.load_from_csv(inpt.from_str(STR_IN), strict=True)
        )
        vt.translate(df)

        pandas.testing.assert_frame_equal(
            pandas.DataFrame({"A": range(10), "B": range(10, 0, -1)}, dtype=str), df,
        )

    def test_vtt_large(self):
        df = pandas.DataFrame({"A": range(10), "B": range(10)}, dtype=str)

        df_vtt = pandas.DataFrame({"old-val": range(100), "new-val": range(100,0,-1)}, dtype=str)

        vt = value_translator.ValueTranslator()
        vt.add_vtt("B", value_translator.load_from_df(df_vtt))
        vt.translate(df)

        pandas.testing.assert_frame_equal(
            pandas.DataFrame({"A": range(10), "B": range(100, 100-10, -1)}, dtype=str),
            df,
        )


if __name__ == "__main__":
    unittest.main()
