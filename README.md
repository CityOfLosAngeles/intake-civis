# intake-civis

This is an [intake](https://intake.readthedocs.io/en/latest)
data source for data warehoused in the [Civis](https://www.civisanalytics.com) platform.

## Requirements
```
civis-python
intake
```
## Installation

`intake-civis` is published on PyPI.
You can install it by running the following in your terminal:
```bash
pip install intake-civis
```

## Usage

You can specify Civis schemas and tables using a YAML intake catalog:

```yaml
sources:
  # An entry representing a catalog for an entire schema.
  postgres:
    driver: "civis_schema"
    args:
      database: "City of Los Angeles - Postgres"
      schema: "transporatation"
  # An entry representing a single table
  bike_trips:
    driver: "civis"
    args:
      database: "City of Los Angeles - Postgres"
      table: "bike_trips"
      schema: "transportation"
```

As a convenience, there is also a top-level function which creates a catalog from the
entire Redshift or PostgreSQL databases.

You can create it with
```python
import intake_civis

redshift_cat = intake_civis.open_redshift_catalog()
postgres_cat = intake_civis.open_postgres_catalog()
```
You can then use these catalogs to drill down to different schemas and tables, e.g.:
```python
bike_trips = postgres_cat.transportation.bike_trips.read()
```

For more examples, see this [demo notebook](./examples/example.ipynb).

### Geospatial support

Both Redshift and Postgres support geospatial values.
We can tell the source to read in a table/query as a GeoDataFrame
by passing in a string or list of strings in the `geometry` argument.
You can also pass in a GeoPandas-compatible `crs` argument to set the
coordinate reference system for the GeoDataFrame.
When more than one column is provided, the primary
geometry column for the GeoDataFrame is assumed to be the first in the list.

The `CivisSchema` object attempts to automatically determine the geometry columns
and coordinate reference systems from the database table metadata.

### Ibis support

Sometimes a table might be too large to load the entire thing into memory.
In that case, it is useful to query a subset of the table.
Ibis is a tool that has a pandas-like API for generating SQL queries.
Civis table catalog entries have a `to_ibis()` function which provides a lazy ibis expression.
This can then be used to query a smaller amount of data:

```python
# Get a lazy ibis object
bike_trips = postgres_cat.transportation.bike_trips.to_ibis()

# Subset it
bike_trips_subset = bike_trips[bike_trips.start_datetime > "2019-04-01"]

# Execute the query to get an in-memory dataframe:
df = bike_trips_subset.execute()
```

**Important limitation:** Due to network restrictions on the Civis databases,
you can only use this feature while in the platform. It will be unable to establish
a connection from your local machine.
