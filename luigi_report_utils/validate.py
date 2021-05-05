import itertools
import pandas

from . import records, parallel

import logging

logger = logging.getLogger(f"{__package__}.validate")


def xref_integrity(df_left, on_left, df_right, on_right, ignore_blanks=False):
    """
    Given two dataframes, df_left and df_right, and their respective tuples of key columns, check_xref
    will return a list of any records whose set of keys are found in the left dataframe only.
    """

    # Convert arguments to lists if needed
    on_left = records._maybe_make_list(on_left)
    on_right = records._maybe_make_list(on_right)

    # The identifier to add to each row to indicate which check generated the failure
    failure_label = "xref_integrity[{} == {}]".format(",".join(on_left), ",".join(on_right))

    # Combine the two dataframes trying to match rows together based on the given columns
    # TODO: support colliding column names, remove suffixes=(False, False), and make sure we
    # don't drop a column from the right-hand side that was also in the left-hand side.
    df = pandas.merge(
        df_left[on_left],
        df_right[on_right],
        how="outer",
        left_on=on_left,
        right_on=on_right,
        suffixes=(False, False),
        copy=False,
        indicator=True,
    )

    # We're only interested in the failures. Find rows that were
    # only on the left-hand side.
    df = df.loc[df["_merge"].eq("left_only")]

    # Drop the special _merge column and the user specified 'on_right' columns that were merged in.
    on_right_uniq = [elem for elem in on_right if elem not in on_left]
    df.drop(columns=["_merge"] + on_right_uniq, inplace=True)

    # Don't fail records where the keys are just blank if thats
    # what the user wants
    if ignore_blanks:
        ignore_condition = df[on_left[0]].eq("")
        for key in on_left[1:]:
            ignore_condition &= df[key].eq("")
        df = df.loc[~ignore_condition]

    # Iterate through all the failures and put them in the standard
    # failed records check format.
    def _process_row(row):
        message = "Missing right-hand record matching: " + str(
            {kr: row[kl] for kl, kr in zip(on_left, on_right)}
        )
        result = [failure_label, message]
        result.extend(row)
        return result

    results = parallel.df_apply(df, _process_row, return_df=False)
    failed_records = itertools.filterfalse(lambda x: x is None, results)

    return failed_records


def unique_keys(df, keys=None):
    """Validate that there are only unique combinations of values in the columns specified by `keys`
    """

    df = df.copy().loc[df.duplicated(keys, keep=False)]

    # The identifier to add to each row to indicate which check generated the failure
    failure_label = f"unique_keys[{keys}]"

    def _process_row(row):
        message = "duplicate row"
        if keys is not None:
            message = "duplicate keys: {!r}".format([row[k] for k in keys])
        result = [failure_label, message]
        result.extend(row)
        return result

    results = parallel.df_apply(df, _process_row, return_df=False)
    failed_records = itertools.filterfalse(lambda x: x is None, results)

    return failed_records
