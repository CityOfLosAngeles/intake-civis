"""
Intake driver for a table in Civis.
"""
import civis
from intake.source import base

from . import __version__


class CivisSource(base.DataSource):
    """
    One-shot Civis platform Redshift/Postgres reader.
    """

    name = "civis"
    version = __version__
    container = "dataframe"
    partition_access = False

    def __init__(
        self,
        database,
        sql_expr=None,
        table=None,
        api_key=None,
        civis_kwargs={},
        metadata={},
    ):
        """
        Create the Civis Source.

        Parameters
        ----------
        database: str
            The name of the database in the platform.
        sql_expr: str
            The table name or SQL expression to pass to the database backend.
        """
        self._database = database
        self._table = table
        self._sql_expr = sql_expr
        self._client = civis.APIClient(api_key)
        self._civis_kwargs = civis_kwargs
        self._dataframe = None

        # Only support reading with pandas
        self._civis_kwargs["use_pandas"] = True
        self._civis_kwargs["client"] = self._client

        # Enforce that exactly one of table or sql_expr are provided
        if bool(table) == bool(sql_expr):
            raise ValueError("Must provide a table OR a sql_expr")

        super(CivisSource, self).__init__(metadata=metadata)

    def _load(self):
        """
        Load the dataframe from Civis.
        """
        if self._table:
            self._dataframe = civis.io.read_civis(
                self._table, self._database, **self._civis_kwargs,
            )
        elif self._sql_expr:
            self._dataframe = civis.io.read_civis_sql(
                self._sql_expr, self._database, **self._civis_kwargs,
            )
        else:
            raise Exception("Should not get here")

    def _get_schema(self):
        """
        Get the schema from the loaded dataframe.
        """
        if self._dataframe is None:
            self._load()
        return base.Schema(
            datashape=None,
            dtype=self._dataframe.dtypes,
            shape=self._dataframe.shape,
            npartitions=1,
            extra_metadata={},
        )

    def _get_partition(self, _):
        if self._dataframe is None:
            self._load()
        return self._dataframe

    def read(self):
        """
        Main entrypoint to read data from Civis.
        """
        return self._get_partition(None)

    def to_ibis(self):
        """
        Return a lazy ibis expression into the Civis database.
        This should only work inside of the Civis platform.

        Currently blocked on Civis providing the SQL hostname/URI to the db.
        """
        raise NotImplementedError("Cannot produce an ibis expression")

    def to_dask(self):
        """
        Return a lazy dask dataframe backed by the Civis database.
        This should only work inside of the Civis platform.

        Currently blocked on Civis providing the SQL hostname/URI to the db.
        """
        raise NotImplementedError("Cannot produce a dask dataframe")

    def _close(self):
        self._dataframe = None
