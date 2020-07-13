import unittest
import pandas

from luigi_report_utils import inpt, records


class TestRecords(unittest.TestCase):
    def test_load_csv_basic(self):
        INPT_STR = "A,B,C,D\n0,1,2,3"

        df = records.load_csv(
            inpt.from_str(INPT_STR),
            [
                records.SchemaField("A"),
                records.SchemaField("B"),
                records.SchemaField("C"),
            ],
        )

        self.assertEqual(df.at[0, "A"], "0")
        self.assertEqual(df.at[0, "B"], "1")
        self.assertEqual(df.at[0, "C"], "2")

    def test_load_jsonl_basic(self):
        INPT_STR = '{"A":"0","B":"1","C":"2"}'

        df = records.load_jsonl(
            inpt.from_str(INPT_STR),
            [
                records.SchemaField("A"),
                records.SchemaField("B"),
                records.SchemaField("C"),
            ],
        )

        self.assertEqual(df.at[0, "A"], "0")
        self.assertEqual(df.at[0, "B"], "1")
        self.assertEqual(df.at[0, "C"], "2")

    def test_apply_mappings(self):
        df = pandas.DataFrame(
            {"A_1": range(10), "C_4": range(2, 12), "B_2": range(1, 11)}
        )

        df = records.apply_mappings(
            df,
            [
                # [ index, src_name, dest_name ]
                ["001", "A_1", "A"],
                ["002", "B_2", "B"],
                ["003", "C_4", "C"],
            ],
        )

        pandas.testing.assert_frame_equal(
            pandas.DataFrame({"A": range(10), "B": range(1, 11), "C": range(2, 12)}),
            df,
        )

    def test_apply_exclusion_list_basic(self):
        df = pandas.DataFrame({"A": range(10), "B": range(1, 11)})

        # Filter out excluded rows
        df_exclude = pandas.DataFrame({"B_exclude": [3, 5, 7]})
        records.apply_exclusion_list(df, df_exclude, [("B", "B_exclude"),])

        # Verify results
        pandas.testing.assert_frame_equal(
            df,
            pandas.DataFrame(
                [
                    (0, 1),
                    (1, 2),
                    # (2, 3),
                    (3, 4),
                    # (4, 5),
                    (5, 6),
                    # (6, 7),
                    (7, 8),
                    (8, 9),
                    (9, 10),
                ],
                index=(0, 1, 3, 5, 7, 8, 9),
                columns=("A", "B"),
            ),
        )


if __name__ == "__main__":
    unittest.main()
