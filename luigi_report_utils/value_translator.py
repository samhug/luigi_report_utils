import csv

from collections import OrderedDict


class ValueTranslator:
    """Wraps a set of ValueTranstionTable objects and provides
    facility to translate row-by-row `ValueTranslator.translate_row(...)`
    or batch translate all rows in a datafram `ValueTranslator.translate(...)`
    """

    def __init__(self):
        self.translation_tables = OrderedDict()

    def add_vtt(self, cols, vtt):
        """Add a ValueTranslationTable

        Example:
            translator = ValueTranslator()

            # Replace values in colA 
            translator.add_vtt("colA", ValueTranslationTable({
                ("A",): "a",
                ("B",): "b",
            }))

            # Replace values in colC where colB AND colC match the given values
            translator.add_vtt(["colB", "colC"], ValueTranslationTable({
                ("0", "1"): "X",
                ("1", "4"): "Y",
            }))
        """
        if not isinstance(cols, tuple):
            cols = (cols,)
        self.translation_tables[cols] = vtt

    def translate_row(self, row):
        """Given a `RowProxy` or some other dict-like object, apply our translations to it
        and return the translated row
        """
        for (match_cols, vtt) in self.translation_tables.items():
            match_tuple = tuple([row[col] for col in match_cols])
            newval_col = match_cols[-1]
            if match_tuple in vtt.valmap:
                row[newval_col] = vtt.valmap[match_tuple]
            elif vtt.strict:
                match_dict = {k[0]: k[1] for k in zip(match_cols, match_tuple)}
                raise KeyError(
                    f"VTT lookup failed: VTT contains no key matching {match_dict}"
                )
        return row

    def translate(self, df):
        """Given a datafram, do a batch translation for each translation table we have.

        Returns: None (Modifies the dataframe in-place)
        """
        for (match_cols, vtt) in self.translation_tables.items():
            vtt.translate(df, match_cols)


class ValueTranslationTable:
    """Contains a valmap dict where the key is a tuple of row values that must match
    for us to substitute.
    if strict == True then the value for each row must map, otherwise raise a KeyError
    """

    def __init__(self, valmap, strict=None):
        self.valmap = valmap
        self.strict = strict

    def translate(self, df, match_cols):
        def build_match_cond(match_vals):
            match_cond = df[match_cols[0]].eq(match_vals[0])
            for (col, val) in zip(match_cols[1:], match_vals[1:]):
                match_cond &= df[col].eq(val)
            return match_cond

        newval_col = match_cols[-1]

        # Construct a list of all assignments we want to make before we do
        # any assignments. This is to avoid an accidental double translation.
        defered_assignments = []
        for (match_vals, new_val) in self.valmap.items():
            idx = df.index[build_match_cond(match_vals)]
            if idx.size > 0:
                defered_assignments.append((idx, newval_col, new_val))

        for (idx, col, val) in defered_assignments:
            df.loc[idx, [col]] = val


def load_from_csv(_in, match_cols=("old-val",), newval_col="new-val", strict=None):
    valmap = {}
    with _in.open("r") as f:
        csv_f = csv.DictReader(f)
        for row in csv_f:
            match_tuple = tuple([row[col] for col in match_cols])
            valmap[match_tuple] = row[newval_col]
    vt = ValueTranslationTable(valmap, strict=strict)
    return vt


def load_from_df(df, match_cols=("old-val",), newval_col="new-val", strict=None):
    valmap = {}
    for idx, row in df.iterrows():
        match_tuple = tuple([row[col] for col in match_cols])
        valmap[match_tuple] = row[newval_col]
    vt = ValueTranslationTable(valmap, strict=strict)
    return vt
