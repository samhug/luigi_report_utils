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

    def test_load_flatten(self):
        inpt_str = '{"A":"0","B_MV":[{"B_MS":[{"B":"1"}]}],"C":"2"}'

        df = records.load_jsonl(
            inpt.from_str(inpt_str),
            (
                records.SchemaField("A"),
                records.SchemaField("B_MV", transform=records.flatten_mv),
                records.SchemaField("C"),
            ),
        )

        self.assertEqual(df.at[0, "A"], "0")
        self.assertEqual(df.at[0, "B_MV"], "1")
        self.assertEqual(df.at[0, "C"], "2")

    def test_load_flatten_error(self):
        inpt_str = '{"A":"0","B_MV":[{"B_MS":[{"B":"1"},{"B":"2"}]}],"C":"2"}'

        with self.assertRaises(ValueError):
            df = records.load_jsonl(
                inpt.from_str(inpt_str),
                (
                    records.SchemaField("A"),
                    records.SchemaField("B_MV", transform=records.flatten_mv),
                    records.SchemaField("C"),
                ),
            )

    def test_load_jsonl_transform(self):
        inpt_str = '{"A":"test"}'

        df = records.load_jsonl(
            inpt.from_str(inpt_str),
            (
                records.SchemaField("A", transform=[lambda v: f"{v}-suffix", lambda v: f"prefix-{v}", lambda v: v.upper()]),
            ),
        )

        self.assertEqual(df.at[0, "A"], "PREFIX-TEST-SUFFIX")

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


class TestExpandMV(unittest.TestCase):
    def test_basic(self):
        data_test = """{ "ID": "0", "SUBVAL": [ "0", "1", "2" ] }
{ "ID": "1", "SUBVAL": [ "0", "1", "2" ] }
"""
        data_expected = """{ "ID": "0", "ID_SUB": "0" }
{ "ID": "0", "ID_SUB": "1" }
{ "ID": "0", "ID_SUB": "2" }
{ "ID": "1", "ID_SUB": "0" }
{ "ID": "1", "ID_SUB": "1" }
{ "ID": "1", "ID_SUB": "2" }
"""

        df = records.load_jsonl(
            inpt.from_str(data_test),
            [records.SchemaField("ID"), records.SchemaField("SUBVAL"),],
        )

        df = records.expand_multivalued(df, {"ID_SUB": ["SUBVAL", None],})

        df_expected = records.load_jsonl(
            inpt.from_str(data_expected),
            [records.SchemaField("ID"), records.SchemaField("ID_SUB"),],
        )

        pandas.testing.assert_frame_equal(df_expected, df)

    def test_basic_no_drop(self):
        data_test = """{ "ID": "0", "SUBVAL": [ "0", "1", "2" ] }
{ "ID": "1", "SUBVAL": [ "0", "1", "2" ] }
"""
        data_expected = """{ "ID": "0", "SUBVAL": [ "0", "1", "2" ], "ID_SUB": "0" }
{ "ID": "0", "SUBVAL": [ "0", "1", "2" ], "ID_SUB": "1" }
{ "ID": "0", "SUBVAL": [ "0", "1", "2" ], "ID_SUB": "2" }
{ "ID": "1", "SUBVAL": [ "0", "1", "2" ], "ID_SUB": "0" }
{ "ID": "1", "SUBVAL": [ "0", "1", "2" ], "ID_SUB": "1" }
{ "ID": "1", "SUBVAL": [ "0", "1", "2" ], "ID_SUB": "2" }
"""

        df = records.load_jsonl(
            inpt.from_str(data_test),
            [records.SchemaField("ID"), records.SchemaField("SUBVAL"),],
        )

        df = records.expand_multivalued(
            df, {"ID_SUB": ["SUBVAL", None],}, drop_mv=False
        )

        df_expected = records.load_jsonl(
            inpt.from_str(data_expected),
            [
                records.SchemaField("ID"),
                records.SchemaField("SUBVAL"),
                records.SchemaField("ID_SUB"),
            ],
        )

        pandas.testing.assert_frame_equal(df_expected, df)

    def test_expand_mid(self):
        data_test = """{ "ID": "0", "SUBVAL": [ {"ID_SUB":"0"}, {"ID_SUB":"1"}, {"ID_SUB":"2"} ] }
{ "ID": "1", "SUBVAL": [ {"ID_SUB":"0"}, {"ID_SUB":"1"}, {"ID_SUB":"2"} ] }
"""
        data_expected = """{ "ID": "0", "ID_SUB": "0" }
{ "ID": "0", "ID_SUB": "1" }
{ "ID": "0", "ID_SUB": "2" }
{ "ID": "1", "ID_SUB": "0" }
{ "ID": "1", "ID_SUB": "1" }
{ "ID": "1", "ID_SUB": "2" }
"""

        df = records.load_jsonl(
            inpt.from_str(data_test),
            [records.SchemaField("ID"), records.SchemaField("SUBVAL"),],
        )

        df = records.expand_multivalued(df, {"ID_SUB": ["SUBVAL", None, "ID_SUB"],})

        df_expected = records.load_jsonl(
            inpt.from_str(data_expected),
            [records.SchemaField("ID"), records.SchemaField("ID_SUB"),],
        )

        pandas.testing.assert_frame_equal(df_expected, df)

    def test_expand_empty(self):
        data_test = """{ "ID": "0", "SUBVAL": [] }
{ "ID": "1", "SUBVAL": [ {"ID_SUB":"0"}, {"ID_SUB":"1"}, {"ID_SUB":"2"} ] }
"""
        data_expected = """{ "ID": "1", "ID_SUB": "0" }
{ "ID": "1", "ID_SUB": "1" }
{ "ID": "1", "ID_SUB": "2" }
"""

        df = records.load_jsonl(
            inpt.from_str(data_test),
            [records.SchemaField("ID"), records.SchemaField("SUBVAL"),],
        )

        df = records.expand_multivalued(df, {"ID_SUB": ["SUBVAL", None, "ID_SUB"],})

        df_expected = records.load_jsonl(
            inpt.from_str(data_expected),
            [records.SchemaField("ID"), records.SchemaField("ID_SUB"),],
        )

        pandas.testing.assert_frame_equal(df_expected, df)


if __name__ == "__main__":
    unittest.main()
