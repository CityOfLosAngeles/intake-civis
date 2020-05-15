"""
Intake driver for a table in Civis.
"""
import civis
from intake.catalog.base import Catalog
from intake.catalog.local import LocalCatalogEntry
from intake.source import base

from ._version import __version__


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
            The SQL expression to pass to the database backend. Either this
            or table must be given.
        table: str
            The table name to pass to the database backend. Either this or
            sql_expr must be given.
        api_key: str
            An optional API key. If not given the env variable CIVIS_API_KEY
            will be used.
        civis_kwargs: dict
            Optional kwargs to pass to the civis.io functions.
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


class CivisCatalog(Catalog):
    """
    Makes data sources out of known tables in a Civis database.

    This queries the database for tables (optionally in a given schema)
    and constructs intake sources from that.
    """

    name = "civis_cat"
    version = __version__

    def __init__(
        self, database, schema="public", api_key=None, civis_kwargs={}, **kwargs
    ):
        """
        Construct the Civis Catalog.

        Parameters
        ----------
        database: str
            The name of the database.
        schema: str
            The schema to list (defaults to "public").
        api_key: str
            An optional API key. If not given the env variable CIVIS_API_KEY
            will be used.
        civis_kwargs: dict
            Optional kwargs to pass to the sources.
        """
        self._civis_kwargs = civis_kwargs
        self._database = database
        self._api_key = api_key
        self._client = civis.APIClient(api_key)
        self._dbschema = schema  # Don't shadow self._schema upstream
        kwargs["ttl"] = kwargs["ttl"] or 100  # Bump TTL so as not to load too often.
        super(CivisCatalog, self).__init__(**kwargs)

    def _load(self):
        """
        Query the Civis database for all the tables in the schema
        and construct catalog entries for them.
        """
        fut = civis.io.query_civis(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{self._dbschema}'",
            database=self._database,
            client=self._client,
        )
        res = fut.result()
        tables = [row[0] for row in res.result_rows]
        self._entries = {}
        for table in tables:
            name = f'"{self._dbschema}"."{table}"'
            entry = LocalCatalogEntry(
                name,
                f"Civis table {table} from {self._database}",
                CivisSource,
                True,
                args={
                    "api_key": self._api_key,
                    "civis_kwargs": self._civis_kwargs,
                    "database": self._database,
                    "table": name,
                },
                getenv=False,
                getshell=False,
            )
            self._entries[table] = entry
