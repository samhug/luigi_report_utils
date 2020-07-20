from pathos.pools import ThreadPool
from pathos.helpers import cpu_count


def df_apply(df, f, pool=None, n_cpus=None, return_df=True):
    """Apply the function `f` to each row in `df` in a parallel fashion.
    """
    if pool is None:
        if n_cpus is None:
            n_cpus = cpu_count()
        pool = ThreadPool(n_cpus)

    class RecordProxy:
        """A proxy object to wrap a `DataFrame.iat[row_i, col_i]` access model and
        provide a dictionary style interface.
        """

        __df = df
        __field_names = list(df.columns)

        @classmethod
        def _field_i(cls, name):
            try:
                return cls.__field_names.index(name)
            except ValueError as e:
                raise KeyError(
                    f"key '{name}' not found on record. Available keys are: {cls.__field_names}"
                )

        @classmethod
        def wrap_map_func(cls, f):
            """Wraps the given function to be passed to a map() style function.
            Returns a function that expects to be called with an index value and it will call
            the given function passing it an object with a python dictionary style interface to the row.
            """
            return lambda row_i: f(cls(row_i))

        @property
        def index(self):
            return self.__row_i

        def __init__(self, row_i):
            self.__row_i = row_i

        def __getitem__(self, key):
            i = self._field_i(key)
            return self.__df.iat[self.__row_i, i]

        def __setitem__(self, key, value):
            i = self._field_i(key)
            self.__df.iat[self.__row_i, i] = value

        def get(self, key, value=None):
            try:
                i = self._field_i(key)
                return self.__df.iat[self.__row_i, i]
            except KeyError:
                return value

        def __str__(self):
            parts = ["Record({"]
            fields_repr = []
            for field_name in self.__field_names:
                field_repr = self.__getitem__(field_name).__repr__()
                fields_repr.append(f"'{field_name}': {field_repr}")
            parts.extend(",".join(fields_repr))
            parts.append("})")
            return "".join(parts)

        def dict(self, keys=None):
            if keys is None:
                keys = self.__field_names

            return {
                key: self.__df.iat[self.__row_i, i]
                for i, key in enumerate(self.__field_names)
                if key in keys
            }

        def __iter__(self):
            return (
                self.__df.iat[self.__row_i, i] for i in range(len(self.__field_names))
            )

    results = pool.map(RecordProxy.wrap_map_func(f), range(df.shape[0]))

    if return_df:
        return df
    else:
        return results
