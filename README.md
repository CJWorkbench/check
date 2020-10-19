Developing
----------

SQL Queries are constants, and they don't take parameters. Given a certain
`check-teamslug.sqlite3.lz4` file, to test a query, try this:

```
lz4 -d check-teamslug.sqlite3.lz4  # to create check-teamslug.sqlite3
python -c 'import check; print(check.ITEMS_SQL)' \
   | sqlite3 check-teamslug.sqlite3 \
   -cmd '.eqp on'
```

(The `.eqp on` explains the query plan before executing. The query plan
is important because it flags slowdowns.)

To actually run most of the code, passing params on the command line:

```
python ./check.py (items|conversations|tasks) /path/to/db.sqlite3.lz4
```

This will print a sampling of output data to the console.
