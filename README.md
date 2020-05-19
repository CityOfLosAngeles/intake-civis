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
    driver: "civis_cat"
    args:
      database: "City of Los Angeles - Postgres"
      schema: "transporatation"
  # An entry representing a single table
  bike_trips:
    driver: "civis"
    args:
      database: "City of Los Angeles - Postgres"
      table: "transportation.bike_trips"
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

The `CivisCatalog` object attempts to automatically determine the geometry columns
and coordinate reference systems from the database table metadata.

### Ibis support

TODO
