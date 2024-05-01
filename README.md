# SQrL 🐿️
a lightweight sqlite API for Python
```
pip install sqrl
```

### Getting Started
```python
from sqrl import SQL # database API

db = SQL("sample.db")

db.insert(
    table_name="purchase_orders",
    data={"price": 100.49},
)
```