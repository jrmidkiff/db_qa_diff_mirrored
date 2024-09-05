**_THIS IS A MIRRORED VERSION OF AN INTERNAL PRODUCTION REPOSITORY LAST UPDATED 2024-07-30_**

# db_qa_diff
QA to compare records between database tables

Expect this to take roughly 10 seconds per 50,000 records 
## Installation

```bash
pip install git+https://github.com/CityOfPhiladelphia/db_qa_diff.git
```
### Additional Requirements
`pip install` the following: 
* python database adapters for the specific databases poviders in use such as `psycopg` for PostgreSQL, `cx_oracle` for Oracle, etc. 
* `citygeo_secrets` recommended for set-up - see [here](https://github.com/CityOfPhiladelphia/citygeo_secrets)

## Usage

```python
import db_qa_diff
import sqlalchemy as sa
import citygeo_secrets as cgs

# Setup
def create_postgresql_engine(creds: dict, schema_secret_name: str, host_secret_name: str) -> sa.Engine:
    '''Compose the URL object, create Postgresql engine, and test connection
    - schema_secret_name: Name of secret that contains schema information. Used to make this function dynamic
    - host_secret_name: Name of secret that contains host information. Used to make this function dynamic'''
    
    db_creds = creds[host_secret_name]
    schema_creds = creds[schema_secret_name]
    url_object = sa.URL.create(
        drivername='postgresql+psycopg', # must already pip install psycopg
        username=schema_creds['login'],
        password=schema_creds['password'],
        host=db_creds['host'],
        port=db_creds['port'],
        database=db_creds['database']
    )
    engine = sa.create_engine(url_object)
    engine.connect() # Test connection to ensure correct credentials, as SQLAlchemy uses lazy initialization
    return engine


def create_oracle_engine(creds: dict, schema_secret_name: str, host_secret_name: str) -> sa.Engine:
    '''Compose the URL object, create Oracle engine, and test connection
    - schema_secret_name: Name of secret that contains schema information. Used to make this function dynamic
    - host_secret_name: Name of secret that contains host information. Used to make this function dynamic'''
    
    db_creds = creds[schema_secret_name]
    creds_host = creds[host_secret_name]
    url_object = sa.URL.create(
        drivername='oracle+cx_oracle', # must already pip install cx_oracle
        username=db_creds['login'],
        password=db_creds['password'],
        host=creds_host['host']['hostName'],
        port=creds_host['host']['port'],
        database=creds_host['database']
    )
    engine = sa.create_engine(url_object)
    engine.connect()
    return engine

cgs.set_config(keeper_dir="~") # Set to directory containing `client_config.json`

engine_oracle_ais_sources = cgs.connect_with_secrets(create_oracle_engine, 
    "databridge-oracle/hostname", "GIS_AIS_SOURCES", 
    host_secret_name="databridge-oracle/hostname", schema_secret_name="GIS_AIS_SOURCES"
    )
engine_postgresql_ais_sources = cgs.connect_with_secrets(create_postgresql_engine, 
    "databridge-v2/hostname-testing", "databridge-v2/ais_sources", 
    host_secret_name="databridge-v2/hostname-testing", schema_secret_name="databridge-v2/ais_sources"
    )

# Actual package use
db_qa_diff.recorddiff(
    engine_oracle_ais_sources, engine_postgresql_ais_sources, 
    "usps_cityzip", "usps_alias", "usps_zip4s", 
    ignore_all=['objectid'], 
    ignore_cols={'usps_cityzip': 'cityname'}
    )

# Output printed to stdout
```

### Output
```bash
# Example output printed to console

Table: usps_cityzip
1 newly appear in <engine2.hostname> (0.5% of 184 rows)
# python list of dicts

6 disappear from <engine1.hostname> (3.2% of 189 rows)
# python list of dicts
...

Lap elapsed time: 5 second(s)
```

## Functions
**db_qa_diff.recorddiff**(_engine1_, _engine2_, _*tables_, 
               _ignore_all_ = [], _ignore_cols_ = {}): 

Compare rows between similarly named tables in two different databases. Does not account for indices, primary keys, or other table artifacts

Parameters: 
* _engine1_: sqlalchemy.Engine
    * An SQLAlchemy Engine
* _engine2_: sqlalchemy.Engine
    * An SQLAlchemy Engine  
* _*tables_: str | tuple[str, str]
    * Names of tables to compare. They can take the form of 
        * `'table1', 'table2', ...` if the tables have the same names, or 
        * `('engine1_table1', 'engine2_table1'), ('engine1_table2', 'engine2_table2'), ...` if the tables have different names between databases
    * Specify a particular schema with the syntax 
        * `"<schema_name>.<table_name>"`
* _ignore_all_: list[str] = [] 
    * A list of columns to ignore when comparing tables passed with _*tables_. Frequently this will be columns such as "objectid" or a floating-point "geometry" field subject to different rounding thresholds between databases. No errors are raised if a table does not contain a column to be ignored.
    * This package currently does not understand geometry fields such as "shape", so these must always be ignored. 
* _ignore_cols_: dict[str, str | list[str]] = {} 
    * A dictionary of `{table_name: ["col1", "col2"], table2: ...}` with a list of columns to exclude from the specified table only. 
    * If comparing two tables with different names, then `ignore_cols` will look for table name matches to either table. 

## Notes
* Oracle does not support temporary tables per session before Oracle 18c. Please use a different database provider for `engine2`, or submit a pull request with manager's approval. This package will make a temporary table in `engine2` using the table in `engine1`.
* For the documentation on SQLAlchemy engines - see [here](https://docs.sqlalchemy.org/en/20/tutorial/engine.html)
