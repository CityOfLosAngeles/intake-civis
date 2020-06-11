from typing import Optional

import sqlalchemy

import ibis

from .alchemy import POSTGRES_KIND, REDSHIFT_KIND, get_db_uri


class RedshiftClient(ibis.sql.postgres.client.PostgreSQLClient):

    """
    An ibis Redshift client for use with Civis.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: Optional[int] = 5432,
        database: Optional[str] = "dev",
        url: Optional[str] = None,
        driver: str = "redshift+psycopg2",
    ):
        if url is None:
            if driver != "redshift+psycopg2":
                raise NotImplementedError(
                    "redshift+psycopg2 is currently the only supported driver"
                )
            sa_url = sqlalchemy.engine.url.URL(
                "redshift+psycopg2",
                host=host,
                port=port,
                username=user,
                password=password,
                database=database,
            )
        else:
            sa_url = sqlalchemy.engine.url.make_url(url)

        # Note: we call super using PostgreSQLClient, thereby initializing
        # with the grandparent of this class rather than the parent.
        # We don't want to init with PostgreSQLClient because it doensn't
        # create the engine we want.
        super(ibis.sql.postgres.client.PostgreSQLClient, self).__init__(
            sqlalchemy.create_engine(sa_url, connect_args={"sslmode": "require"})
        )
        self.database_name = sa_url.database


def get_postgres_ibis_connection(api_key=None):
    url = get_db_uri(POSTGRES_KIND, api_key)
    return ibis.postgres.connect(url=url)


def get_redshift_ibis_connection(api_key=None):
    url = get_db_uri(REDSHIFT_KIND, api_key)
    return RedshiftClient(url=url)
