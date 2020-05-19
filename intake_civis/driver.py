"""
Intake driver for a table in Civis.
"""
import concurrent
import warnings

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
        geometry=None,
        crs=None,
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
        geometry: str or list of str
            A column or list of columns that should be interpreted as geometries.
        crs: str or dict
            A coordinate reference string of the format that GeoPandas can understand.
            Only relevant if geometry columns are given.
        api_key: str
            An optional API key. If not given the env variable CIVIS_API_KEY
            will be used.
        civis_kwargs: dict
            Optional kwargs to pass to the civis.io functions.
        """
        self._database = database
        self._table = table
        self._sql_expr = sql_expr
        self._geom = [geometry] if isinstance(geometry, str) else geometry
        self._crs = crs
        self._client = civis.APIClient(api_key)
        self._civis_kwargs = civis_kwargs
        self._dataframe = None

        if crs and not geometry:
            warnings.warn("A CRS was provided but no geometry columns")

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
        # Load the data.
        if self._table:
            df = civis.io.read_civis(self._table, self._database, **self._civis_kwargs,)
        elif self._sql_expr:
            df = civis.io.read_civis_sql(
                self._sql_expr, self._database, **self._civis_kwargs,
            )
        else:
            raise Exception("Should not get here")

        # If we have geometry columns, convert them with shapely
        # and make the result a GeoDataFrame
        if self._geom and len(self._geom):
            import geopandas
            import shapely

            geom_cols = {
                g: df[g].apply(lambda x: shapely.wkb.loads(x, hex=True))
                for g in self._geom
            }
            self._dataframe = geopandas.GeoDataFrame(
                df.assign(**geom_cols), geometry=self._geom[0], crs=self._crs
            )
        else:
            self._dataframe = df

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
        self,
        database,
        schema="public",
        api_key=None,
        civis_kwargs={},
        has_geometry_column_table=None,
        **kwargs,
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
        has_geometry_column_table: bool
            Whether the database has a "geometry_columns" table, which can be used
            to query for SRID information for a given column. Otherwise we try to
            infer based on whether it is a postgres database.
        civis_kwargs: dict
            Optional kwargs to pass to the sources.
        """
        self._civis_kwargs = civis_kwargs
        self._database = database
        self._api_key = api_key
        self._client = civis.APIClient(api_key)
        if has_geometry_column_table is not None:
            self._has_geom = has_geometry_column_table
        else:
            self._has_geom = "redshift" not in self._database.lower()
        self._dbschema = schema  # Don't shadow self._schema upstream
        kwargs["ttl"] = (
            kwargs.get("ttl") or 100
        )  # Bump TTL so as not to load too often.
        super(CivisCatalog, self).__init__(**kwargs)

    def _load(self):
        """
        Query the Civis database for all the tables in the schema
        and construct catalog entries for them.
        """
        fut1 = civis.io.query_civis(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{self._dbschema}'",
            database=self._database,
            client=self._client,
        )
        # If the database has a geometry_columns table, we prefer that as we can
        # get the SRID for a column from it. Otherwise, we get the geometry columns
        # from the information schema.
        if self._has_geom:
            fut2 = civis.io.query_civis(
                "SELECT f_table_name, f_geometry_column, srid FROM geometry_columns "
                f"WHERE f_table_schema = '{self._dbschema}'",
                database=self._database,
                client=self._client,
            )
        else:
            fut2 = civis.io.query_civis(
                "SELECT table_name, column_name FROM information_schema.columns "
                f"WHERE table_schema = '{self._dbschema}' and udt_name = 'geometry'",
                database=self._database,
                client=self._client,
            )
        done, _ = concurrent.futures.wait((fut1, fut2))
        assert fut1 in done and fut2 in done
        res1 = fut1.result()
        res2 = fut2.result()

        tables = [row[0] for row in res1.result_rows]
        self._entries = {}
        for table in tables:
            name = f'"{self._dbschema}"."{table}"'
            geometry = [r[1] for r in res2.result_rows if r[0] == table]
            srid = [r[2] for r in res2.result_rows if r[0] == table and self._has_geom]
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
                    "geometry": geometry if len(geometry) else None,
                    "crs": f"EPSG:{srid[0]}" if len(srid) else None,
                },
                getenv=False,
                getshell=False,
            )
            self._entries[table] = entry
