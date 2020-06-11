"""
Functions related to creating SQLAlchemy engines for Civis data warehouses.
"""
import os
import typing
from urllib.parse import quote_plus, urlparse

import civis
import sqlalchemy

POSTGRES_KIND = "RemoteHostTypes::Postgres"
REDSHIFT_KIND = "RemoteHostTypes::Redshift"


def get_db_uri(kind: str, api_key: typing.Optional[str] = None) -> str:
    """
    Get a SQLAlchemy-compatble database URL for the Civis data warehouses.

    Parameters
    ==========
    kind: str
        Either "RemoteHostTypes::Postgres" or "RemoteHostTypes::Redshift"
    api_key: Optional[str]
        An optional API key. If not provided, uses the environment variable
        CIVIS_API_KEY.

    Returns
    =======
    uri: str
        A SQLAlchemy-compatible database URL for the requested data warehouse.
    """
    # List all the remote hosts, which include the DB instances
    client = civis.APIClient(api_key)
    hosts = client.remote_hosts.list()

    if kind == REDSHIFT_KIND:
        scheme = "redshift+psycopg2"
    elif kind == POSTGRES_KIND:
        scheme = "postgres"
    else:
        raise ValueError("Unsupported database kind")

    # Find the right DB
    db = next(h for h in hosts if h["type"] == kind)

    # Get the credentials from the environment. First, we check if
    # the database connection data has been injected as env variables.
    # If not, we check the environment for the default user credentials.
    for key, value in os.environ.items():
        if key.endswith("_ID") and value == str(db["id"]):
            # If we found a database connection ID, that should be all
            # we need.
            name = key[:-3]
            user = os.environ.get(f"{name}_CREDENTIAL_USERNAME")
            password = os.environ.get(f"{name}_CREDENTIAL_PASSWORD")
            if not user or not password:
                raise RuntimeError(f"Could not find database credentials for {db}")
            user = quote_plus(user)
            password = quote_plus(password)

            host = os.environ.get(f"{name}_HOST")
            port = os.environ.get(f"{name}_PORT")
            database = os.environ.get(f"{name}_DATABASE")
            uri = f"{scheme}://{user}:{password}@{host}:{port}/{database}"
            break
    else:
        # If we got here, we didn't find a database connection ID.
        # Next, look for database credentials that are injected into
        # scripts as parameters.
        url = db["url"]
        if url.startswith("jdbc:"):
            url = url[5:]
        parsed = urlparse(url)
        credential = client.credentials.get(client.default_credential)
        for key, value in os.environ.items():
            if key.endswith("_ID") and value == str(client.default_credential):
                name = key[:-3]
                user = os.environ.get(f"{name}_USERNAME")
                password = os.environ.get(f"{name}_PASSWORD")
                break
        else:
            # If we got here, there were not database credentials as parameters,
            # and we are probably in a notebook setting. Check to see if there
            # are credentials attached with the user name.
            user = os.environ.get(f"{credential['name'].upper()}_USERNAME")
            password = os.environ.get(f"{credential['name'].upper()}_PASSWORD")
        if not user or not password:
            raise RuntimeError(f"Could not find database credentials for {db}")
        user = quote_plus(user)
        password = quote_plus(password)
        uri = f"{scheme}://{user}:{password}@{parsed.netloc}{parsed.path}"
    return uri


def get_postgres_engine(api_key: typing.Optional[str] = None):
    """
    Get a SQLAlchemy engine for a Civis PostgreSQL instance.

    Parameters
    ==========
    api_key: Optional[str]
        An optional API key. If not provided, uses the environment variable
        CIVIS_API_KEY.

    Returns
    =======
    engine: sqlalchemy.Engine
        A SQLAlchemy engine for the PostgreSQL data warehouse.
    """
    uri = get_db_uri(POSTGRES_KIND, api_key)
    return sqlalchemy.create_engine(uri)


def get_redshift_engine(api_key: typing.Optional[str] = None):
    """
    Get a SQLAlchemy engine for a Civis Redshift instance.

    Parameters
    ==========
    api_key: Optional[str]
        An optional API key. If not provided, uses the environment variable
        CIVIS_API_KEY.

    Returns
    =======
    engine: sqlalchemy.Engine
        A SQLAlchemy engine for the Redshift data warehouse.
    """
    uri = get_db_uri(REDSHIFT_KIND, api_key)
    return sqlalchemy.create_engine(uri, connect_args={"sslmode": "require"})
