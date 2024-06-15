# SQrL 🐿️
a lightweight sqlite API for Python

### Getting Started
```python
from sqrl import SQL # database API
db = SQL("sample.db")
```

```python
# still write raw sql using fetch
db.fetch("SELECT title, duration, artist_id FROM tracks ORDER BY track_number;")

# or with the select command
db.select(
  "tracks",
  ["title", "duration", "artist_id"],
  order_by="track_number"
)
```
