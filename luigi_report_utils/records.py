import csv
import json
import itertools

import pandas

from . import parallel

import logging

logger = logging.getLogger(f"{__package__}.records")


class SchemaField:
    def __init__(
        self,
        name,
        type=None,
        transform=None,
        filter_none=False,
        none_value="",
    ):
        self.name = name
        self.type = type
        self.filter_none = filter_none
        self.transform = transform
        self.none_value = none_value

def load_records(records, field_defs, index=None, pool=None):
    """ Given an iterator of dictionary records and a list of field deffinitions,
    will return a DataFrame.
    """

    columns = [field.name for field in field_defs]

    blank_record = [None] * len(field_defs)

    def _process_record(src):
        record = blank_record.copy()

        field_i = 0
        for field_def in field_defs:
            # Retrieve the field from the record, set to None if
            # the field doesn't exist.
            value = src.get(field_def.name, None)

            if field_def.transform is not None:
                value = field_def.transform(value)

            if field_def.filter_none is True and value is None:
                return

            if field_def.none_value is not None and value is None:
                value = field_def.none_value

            record[field_i] = value
            field_i += 1

        return record

    # Process each record, filter out None values
    records = map(_process_record, records)
    records = itertools.filterfalse(lambda x: x is None, records)

    # Construct DataFrame from records
    df = pandas.DataFrame.from_records(records, index=index, columns=columns)

    # Set the DataFrame column types if they were provided
    for field_def in field_defs:
        if field_def.type is not None:
            df[field_def.name] = df[field_def.name].astype(field_def.type)

    return df


def load_csv(inpt, field_defs=None, **kwargs):
    logger.info(f"Loading records from {inpt}")

    csv.register_dialect("strict", strict=True)

    with inpt.open("r") as input_file:
        r = csv.DictReader(input_file, dialect="strict")

        # generate a default list of field_defs with all columns if we weren't given one
        if field_defs is None:
            field_defs = [
                SchemaField(fieldname, type="str") for fieldname in r.fieldnames
            ]

        df = load_records(r, field_defs, **kwargs)

    logger.info(f"Loaded {df.shape[0]} records from {inpt}")
    return df


def save_csv(output, df):
    logger.info(f"Outputing {df.shape[0]} records to {output}")
    with output.open("w") as f:
        ret = df.to_csv(f, index=False)
        logger.info("Output completed.")
        return ret


def load_jsonl(inpt, field_defs, **kwargs):
    logger.info(f"Loading records from {inpt}")

    with inpt.open("r") as input_file:
        raw_records = map(json.loads, input_file)
        df = load_records(raw_records, field_defs, **kwargs)

    logger.info(f"Loaded {df.shape[0]} records from {inpt}")
    return df


def save_jsonl(output, df):
    logger.info(f"Outputing {df.shape[0]} records to {output}")

    ret = df.to_json(output, orient="records", lines=True)

    logger.info("Output completed.")
    return ret


def apply_exclusion_list(df, f, match_tuples):
    """ Given a DataFrame, an exclusion list input file, and a list of tuples containing column xrefs,
    will iterate through each record in the DataFrame excluding any record where the columns specified
    in the match_tuples correspond to a row in the exclusion list.

    The match_tuples should be specified as follows:
      match_tuples = (
        ('key1', 'exclusion_key1'),
        ('key2', 'exclusion_key2'),
      )
    """
    df_exclude = None
    if isinstance(f, pandas.DataFrame):
        # use the DataFrame we were given
        df_exclude = f
    else:
        # Load the exclusion DataFrame from csv
        df_exclude = load_csv(f)

    # Validate the match tuples
    for match_tuple in match_tuples:
        if match_tuple[0] not in df.columns:
            raise KeyError(
                "column {} doesn't exist in the target dataframe".format(
                    match_tuple[0].__repr__()
                )
            )
        if match_tuple[1] not in df_exclude.columns:
            raise KeyError(
                "column {} doesn't exist in the exclusion list".format(
                    match_tuple[0].__repr__()
                )
            )

    # Create an index to hold references to the rows we want to drop
    drop_i = pandas.Int64Index([])

    for _, row in df_exclude.iterrows():
        # Construct a drop condition that checks if the values of the columns of interest
        # in the dataframe match a row in the exclusion list.
        drop_condition = df[match_tuples[0][0]] == row[match_tuples[0][1]]
        for match_tuple in match_tuples[1:]:
            drop_condition &= df[match_tuple[0]] == row[match_tuple[1]]

        # Add references to all rows that match the drop condition to the drop index
        drop_i = drop_i.append(df[drop_condition].index)

    df.drop(index=drop_i, inplace=True)



def expand_multivalued(df, expansion_paths, drop_mv=True):
    """
    Given a DataFrame like the following:
      ID 	ID_SUB_MV
      0     [{'ID_SUB': '0', 'VAL': 'a'}, {'ID_SUB': '1', 'VAL': 'b' }, { 'ID_SUB': '2', 'VAL': 'c' }]
      1     [{'ID_SUB': '0', 'VAL': 'A'}, {'ID_SUB': '1', 'VAL': 'B' }, { 'ID_SUB': '2', 'VAL': 'C' }]
      2     [{'ID_SUB': '0', 'VAL': '0'}, {'ID_SUB': '1', 'VAL': '1' }, { 'ID_SUB': '2', 'VAL': '2' }]

    > expand_multivalued(df, {
        'ID_SUB': ('ID_SUB_MV', None, 'ID_SUB'),
        'VAL':    ('ID_SUB_MV', None, 'VAL'),
    })

    Will return a DataFrame of the form:
      ID 	ID_SUB  VAL
      0     '0'     'a'
      0     '1'     'b'
      0     '2'     'c'
      1     '0'     'A'
      1     '1'     'B'
      1     '2'     'C'
      2     '0'     '0'
      2     '1'     '1'
      2     '2'     '2'
    """

    # Create new blank columns to expand values into
    new_columns = {new_col: None for new_col, path in expansion_paths.items()}
    df = df.assign(**new_columns)

    # Custruct list of column names that will be in the output DataFrame
    target_columns = list(df.columns)
    if drop_mv:
        for path in expansion_paths.values():
            if path[0] in target_columns:
                target_columns.remove(path[0])

    def _process_row(row):
        results = []
        sub_lengths = []

        for col, path in expansion_paths.items():
            # Step through the key path. None values represent a wildcard
            val = row[path[0]]
            for key in path[1:]:
                if key is None:
                    sub_lengths.append(len(val))
                    break
            else:
                raise KeyError(
                    "key path must contain a None value to use as a wildcard"
                )

        # Constuct a skeleton row containing all keys that will
        # be in the target DataFrame
        row_skeleton = row.dict(keys=target_columns)

        for row_i in range(max(sub_lengths)):
            for col, path in expansion_paths.items():

                # Step through the key path. None values represent a wildcard
                val = row[path[0]]
                for key in path[1:]:
                    if val is None:
                        continue

                    if key is None:
                        val = val[row_i] if row_i < len(val) else None
                    else:
                        val = val.get(key, None)

                if len(results) <= row_i:
                    results.append(row_skeleton.copy())
                results[row_i][col] = val

        return results

    results = parallel.df_apply(df, _process_row, return_df=False)
    raw_records = itertools.chain.from_iterable(
        itertools.filterfalse(lambda x: x is None, results)
    )

    field_defs = [SchemaField(col) for col in target_columns]

    return load_records(raw_records, field_defs)


# mappings in the form: [ [ 'index', 'src_name', 'dest_name' ], ... ]
def apply_mappings(df, mappings):
    def _assert_no_duplicates(l, m):
        s = set()
        duplicates = set(x for x in l if x in s or s.add(x))
        assert len(duplicates) == 0, f"{m}: duplicates found {duplicates}"

    # Make sure the mappings are sorted by index
    mappings = sorted(mappings, key=lambda mapping: mapping[0])

    _assert_no_duplicates([m[0] for m in mappings], "mapping index")

    # Make sure all src_names exists in the DataFrame
    for m in mappings:
        assert m[1] in df.columns, f"Column not found in dataframe: '{m[1]}'"

    # Rename the columns
    col_mapings = {m[1]: m[2] for m in mappings}
    df.rename(columns=col_mapings, inplace=True)

    # Re-Order the columns
    col_order = [m[2] for m in mappings]
    _assert_no_duplicates(col_order, "dest_field names")
    return df.reindex(columns=col_order, copy=False)
