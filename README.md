# SQrL 🐿️
a lightweight sqlite API for Python

### Getting Started
```python
from sqrl import SQL # database API
db = SQL("sample.db")
```
### Quick Intro
#### Select Statements
```python
from sqrl import SQL

db = SQL("sample.db")

db.select(
    "track", ["title", "duration", "artist_id"], order_by="track_number"
)  # SELECT title, duration, artist_id FROM track ORDER BY track_number;

db.select("album")  # SELECT * FROM album;

# get as funky as you want with it
db.select(
    "album ab join artist ar on ab.artist_id = ar.id",
    ["ab.title", "ar.name"],
    order_by="year",
    asc=False,
    where="ab.genre = 'soul'",
    limit=10,
    return_as_dict=True
)
# set 'row_factory' to None to get value tuples
# or return_as_dict to get dictionary objects


# and of course, writing completely raw sql still available
# reccomended for parameterization at the moment
db.fetch("SELECT * FROM album WHERE year >= ? AND artist_id = ?;", 2000, 57)

# you specify whether fetchall, fetchone or fetchmany is used with 'n'
db.fetch("SELECT * FROM artist WHERE id = ?", 57, n=1, return_as_dict=True)
```
#### CrUD
insert, update and delete from tables using dedicated methods.
built with Flask and JSON REST APIs in mind.
```python
from sqrl import SQL

db = SQL("sample.db")

# insert into database
db.insert(
    table_name="album",
    data={
        "id": 1,
        "title": "myalbum",
        "artist_id": 10,
        "year": 2024,
        "genre": "alternative"
    }
)
# parameterization safe! generated under the hood ->
db.execute(
    "INSERT INTO album (id, title, artist_id, year, genre) VALUES (?, ?, ?, ?, ?);",
    1, "myalbum", 10, 2024, "alternative",
    as_transaction=True
)

# update something in a table
db.update(
    table_name="album",
    data={
        "title": "a new title"
    },
    where=f"id = {1}"
)
# delete from table
db.delete(
    table_name="album", where=f"id = {1}"
)
# although the param dictionary for update is parameterized, the where clause is not. so you can use db.execute
# to execute full SQL statements i.e. db.execute("DELETE FROM album WHERE id = ?", 1)
```

#### Aggregations
perform aggregations on a chosen table with dedicated methods
```python
from sqrl import SQL

db = SQL("sample.db")
db.max(table_name="invoice", column="amount")
db.min("invoice", "amount")
db.avg("invoice", "amount", 2)  # ROUND(AVG(amount), 2)
db.count("invoice")
db.sum("invoice", "amount")
```

#### Exporting
```python
from sqrl import SQL

db = SQL("sample.db")

db.export_to_csv()  # exports every table in database to csv as [tablename].csv
db.export_table_to_csv("tracks")  # export just a single table
db.dump()  # writes the entire database schema to .sql, can specify 'out_file' by default will by named after database file
```