import sqlalchemy as sa
from .utils import SimpleTimer
import pprint, re
from dataclasses import dataclass, field
from collections.abc import Sequence

@dataclass
class _Bucket: 
    '''A bucket class to keep track of everything involved with comparing two tables
    in potentially separate databases'''
    t1name: str
    t2name: str
    metadata1: sa.MetaData
    metadata2: sa.MetaData
    host1: str
    host2: str
    engine1: sa.Engine
    engine2: sa.Engine
    table1: sa.Table
    table2: sa.Table
    table1_in_engine2: sa.Table = field(init=False) # Added later
    drop_cols: list[str]


def _create_table1_in_engine2(b: _Bucket) -> sa.Table: 
    '''Create table1 as a TEMP table in engine2 database'''
    l = []
    for col in b.table2.c:
        if col.name not in b.drop_cols:
            l.append(sa.Column(col.name, col.type))
    table1_in_engine2 = sa.Table(f'{b.table1}_table1_in_engine2',
                                 b.metadata2, *l, prefixes=['TEMPORARY'])
    table1_in_engine2.create(bind=b.engine2)
    return table1_in_engine2


def _copy_table1_to_engine2(b: _Bucket, conn1: sa.Connection, conn2: sa.Connection):
    '''Batch INSERT data from table1 in engine1 to the temp table in engine2'''
    n = 15000
    rows_inserted = 0
    print(f'\n{"*" * 80}')
    print(f'Transferring {b.t1name} from {b.host1} to TEMP table in {b.host2}')
    stmt = sa.select(sa.func.count()).select_from(b.table1)
    result = conn1.execute(stmt)
    row_count = result.scalar_one_or_none()

    conn1.execution_options(yield_per=n)
    stmt = sa.select(b.table1)
    result = conn1.execute(stmt)
    for partition in result.mappings().partitions():
        stmt2 = sa.insert(b.table1_in_engine2)
        conn2.execute(stmt2, partition)
        rows_inserted = rows_inserted + len(partition)
        print(f'... transferred {rows_inserted:,d} of {row_count:,d} - {rows_inserted / row_count:.1%}')
    print()


def _create_drop_cols(ignore_all: list[str], ignore_cols: dict, entry: str) -> list[str]: 
    '''Create the columns to ignore from all tables and 
    the columns to ignore from this specific table'''
    drop_cols = [l.lower() for l in ignore_all]
    extend_cols = []
    if isinstance(entry, str): 
        entry = [entry]
    for k, v in ignore_cols.items(): 
        for table in entry: 
            if k == table: 
                if isinstance(v, list) or isinstance(v, tuple): 
                    extend_cols.extend(v)
                else: 
                    extend_cols.append(v)
    drop_cols.extend(extend_cols)
    return drop_cols


def _compare_tables(b: _Bucket, conn2: sa.Connection): 
    '''Compare the two tables now in engine2 with SQL EXCEPT'''
    table_2_cols = [col for col in b.table2.c if col.name not in b.drop_cols]

    stmt_appear = sa.select(
        *table_2_cols).except_(sa.select(b.table1_in_engine2))
    table1_in_engine2_nrows = conn2.execute(
        sa.select(sa.func.count()).select_from(b.table1_in_engine2)).scalar_one()
    rv_appear = conn2.execute(stmt_appear)
    appear = rv_appear.mappings().fetchmany(5)
    
    stmt_disappear = (sa.select(b.table1_in_engine2).except_(
        sa.select(*table_2_cols)))
    table2_nrows = conn2.execute(
        sa.select(sa.func.count()).select_from(b.table2)).scalar_one()
    rv_disappear = conn2.execute(stmt_disappear)
    disappear = rv_disappear.mappings().fetchmany(5)

    print(f'{b.t2name}: {max(rv_appear.rowcount, 0):,d} newly appear in {b.host2} ({max(rv_appear.rowcount / max(table2_nrows, 1), 0) :.1%} of {table2_nrows:,d} rows)')
    if rv_appear.rowcount > 0: 
        for row in appear: 
            pprint.pprint(dict(row), sort_dicts=False)
    print()
    print(f'{b.t1name}: {max(rv_disappear.rowcount, 0):,d} disappear from {b.host1} ({max(rv_disappear.rowcount / max(table1_in_engine2_nrows, 1), 0) :.1%} of {table1_in_engine2_nrows:,d} rows)')
    if rv_disappear.rowcount > 0: 
        for row in disappear: 
            pprint.pprint(dict(row), sort_dicts=False)


def recorddiff(engine1: sa.Engine, engine2: sa.Engine, 
               *tables: str | tuple[str, str], 
               ignore_all: list[str] = [], ignore_cols: dict[str, str | list[str]] = {}): 
    '''Compare rows between similarly named tables in two different databases
    
    Does not account for indices, primary keys, or other table artifacts
    - `engine1`: SQLAlchemy Engine
    - `engine2`: SQLAlchemy Engine (must not be Oracle)
    - `tables`: Names of tables to compare. They can take the form of 
        - `'table1', 'table2', ...` if the tables have the same names, or 
        - `[('engine1_table1', 'engine2_table1'), ('engine1_table2', 'engine2_table2'), ...]` if the tables have different names between databases
    - `ignore_all`: List of columns to ignore across all tables in comparison
    - `ignore_cols`: Dict of `{table: [list of columns]}` to ignore only in a specific table
    
    It is advised to put the older database first, i.e. if you are comparing an
    older Oracle database and a modern postgresql database, then `engine1` should be 
    Oracle while `engine2` should be postgresql. This package will make a table in 
    `engine2` using the table in `engine1`, meaning you may run into name length 
    overflow errors if `engine2` is an older database.'''
    if engine2.name == 'oracle': 
        raise NotImplementedError("Oracle does not support temporary tables per session before Oracle 18c. Please use a different database provider for engine2, or submit a pull request with manager's approval.")
    timer = SimpleTimer()
    metadata1 = sa.MetaData()
    metadata2 = sa.MetaData()
    assert isinstance(ignore_all, list), "ignore_all is not a list!"
    assert isinstance(ignore_cols, dict), "ignore_cols is not a dict!"

    for entry in tables: 
        timer.start_lap()
        if isinstance(entry, str): 
            t1name = entry
            t2name = entry
        elif isinstance(entry, Sequence): 
            assert len(entry) == 2
            t1name, t2name = entry[0], entry[1]
        else: 
            raise TypeError(f'Type of {entry} ({type(entry)}) not accepted. Must be str or sequence with length 2.')

        regex_pattern = '^(?:(?P<schema>\w+)\.)?(?P<table>\w+)$'
        try: 
            m1 = re.match(regex_pattern, t1name)
            schema_extract1 = m1['schema']
            table_extract1 = m1['table']
            table1 = sa.Table(table_extract1, metadata1, schema=schema_extract1, autoload_with=engine1)
            host1 = engine1.url.host
        except sa.exc.NoSuchTableError as e: 
            print(f'\nTable "{t1name}" not found in {engine1.url}\n')
            raise e
        try: 
            m2 = re.match(regex_pattern, t2name)
            schema_extract2 = m2['schema']
            table_extract2 = m2['table']
            table2 = sa.Table(table_extract2, metadata2, schema=schema_extract2, autoload_with=engine2)
            host2 = engine2.url.host
        except sa.exc.NoSuchTableError as e: 
            print(f'\nTable {t2name} not found in {engine2.url}\n')
            raise e

        drop_cols = _create_drop_cols(ignore_all, ignore_cols, entry)

        b = _Bucket(t1name, t2name, metadata1, metadata2, host1, host2, engine1, engine2, 
                   table1, table2, drop_cols)
        b.table1_in_engine2 = _create_table1_in_engine2(b)
        # To see what b looks like now, use pprint.pprint(b)

        with engine1.begin() as conn1, engine2.begin() as conn2: 
            _copy_table1_to_engine2(b, conn1, conn2)
            _compare_tables(b, conn2)

        timer.end_lap()
    
    timer.end()
