"""
Oliver 2024
"""

import csv
import os.path
import sqlite3 as sql
from typing import Dict, List, Any, Tuple

TABLE_NAME_INDEX = 0


def process_dict(d: Dict[Any, Any]) -> Tuple[List[Any], List[Any]]:
    """
    dictionary helper that splits the key and values into lists
    :param d: a dictionary
    :return: tuple of key list and values list
    """
    keys = list(d.keys())
    vals = list(d.values())
    return keys, vals


class SQL:
    def __init__(self, filename: str, debug: bool = False):
        """
        :param filename: path to database file
        :param debug: flag of whether to print errors to console
        """
        self.file: str = filename
        if not os.path.exists(self.file):
            raise FileNotFoundError
        self.debug: bool = debug
        self.schema: Dict[str, List[str]] = {}
        self.build_schema()

    def get_table_names(self) -> List[str]:
        """
        returns a list of all table names in the database
        :return: list of strings of table names
        """
        tables = self.fetch(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;",
            row_factory=None
        )
        return [t[0] for t in tables]

    def get_column_names(self, table_name: str) -> List[str]:
        """
        returns a list of all column names in a given table
        :param table_name: name of table in database
        :return: list of strings
        """
        columns = self.fetch(
            f"PRAGMA table_info({table_name});",
            row_factory=None
        )
        return [col[1] for col in columns]

    def build_schema(self) -> None:
        """
        creates a quick lookup dictionary for
        the basic schema of the database
        with table names and keys and a list of
        column names as values
        :return: None
        """
        self.schema.clear()

        for table in self.get_table_names():
            table_name = table[TABLE_NAME_INDEX]
            # get all of the current tables columns
            self.schema[table_name] = self.get_column_names(table_name)

    def get_con(self) -> sql.Connection:
        """
        returns sqlite connection object for
        the database
        :return: sqlite3 connection
        """
        return sql.connect(self.file)

    def select(self, table_name: str,
               columns: List[str] | None = None,
               distinct: bool = False,
               where: str | None = None,
               order_by: str | None = None,
               asc: bool = True,
               group_by: str | None = None,
               having: str | None = None,
               limit: int = -1,
               offset: int = 0,
               fetch: None | int = None,
               row_factory: sql.Row | None = sql.Row
               ) -> List[Tuple[Any]] | List[sql.Row]:
        """
        buildabe select statement shortcut,
        returns a given number of rows that matched the
        given criteria in either dict convertible Row format
        or value tuples
        :param table_name: name of the table to select from
        :param columns: (optional) columns to return in selection, if None then all
        :param distinct: flag whether to select distinct columns
        :param where: (optional) where conditional string statement
        :param order_by: (optional) column name to order results by
        :param asc: flag of where to order in ascending or descending fashion (default: True)
        :param group_by: (optional) group by clause string
        :param having: (optional) having clause string
        :param limit: int to limit rows returned (default: -1 AKA no limit)
        :param offset: offset from where to start rows that are returned (default: 0)
        :param fetch: number of items to fetch from return (default: None/all)
        :param row_factory: return a dict converitable rows or set to None to return tuples
        :return: list of items from select
        """
        # format columns
        if columns:
            col_list = ",".join(columns)
        else:
            col_list = "*"

        # build select statement
        stmt = (
            f"SELECT {'DISTINCT' if distinct else ''}{col_list} FROM {table_name}"
            f"{'' if not where else f" WHERE {where}"}"
            f"{'' if not order_by else f" ORDER BY {order_by} {'ASC' if asc else 'DESC'}"}"
            f" LIMIT {limit} OFFSET {offset}"
            f"{'' if not group_by else f" GROUP BY {group_by}"}"
            f"{'' if not having else f" HAVING {having}"};"
        )
        result = self.fetch(stmt, n=fetch, row_factory=row_factory)
        return result

    def insert(self, table_name: str, data: Dict[str, Any], replace: bool = False) -> bool:
        """
        insert (or replace) new data into a table
        :param table_name: name of table in database
        :param data: a dictionary of column names and the values
        :param replace: flag of whether make it an OR REPLACE statement
        :return: boolean whether execution was successful
        """
        if not self.table_exists(table_name):
            return False
        columns, values = process_dict(data)
        core = "INSERT INTO" if not replace else "INSERT OR REPLACE INTO"
        col_list = ','.join(columns)
        val_list = ','.join(map(lambda _: '?', values))
        stmt = f"{core} {table_name} ({col_list}) VALUES ({val_list});"

        success = self.execute(stmt, *values, as_transaction=True)
        self.__vacuum()
        return success

    def update(self, table_name: str, data: Dict[str, Any], where: str = "1 = 1") -> bool:
        """
        update data within a given table
        :param table_name: table name to update in
        :param data: dictionary of column names and new values
        :param where: conditional clause for updating (default: 1 = 1 / update everything)
        :return: boolean whether execution was successful
        """
        if not self.table_exists(table_name):
            return False
        columns, values = process_dict(data)

        params = ', '.join(map(lambda c: f"{c} = ?", columns))
        stmt = f"UPDATE {table_name} SET {params} WHERE {where};"

        success = self.execute(stmt, *values, as_transaction=True)
        self.__vacuum()
        return success

    def delete(self, table_name: str, where: str) -> bool:
        """
        delete from a table based on a given conditional
        :param table_name: name of table in database
        :param where: conditional for which item to delete from table
        :return: boolean of whether execution was successful
        """
        if not self.table_exists(table_name):
            return False

        stmt = f"DELETE FROM {table_name} WHERE {where};"

        success = self.execute(stmt, as_transaction=True)
        self.__vacuum()
        return success

    def table_exists(self, name: str) -> bool:
        """
        return whether a table name is found in database
        :param name: name of table
        :return: True if exists else False
        """
        return name in self.get_table_names()

    def column_exists_in_table(self, table_name: str, column: str) -> bool:
        """
        return if a column name exists within a given table
        :param table_name: name of table
        :param column: column name
        :return: True if column is in table else False
        """
        if not self.table_exists(table_name):
            return False
        return column in self.schema[table_name]

    def fetch_first_value(self, __sql: str, *__params) -> Any:
        """
        special case of fetchone where you only want one row and
        the first value of the first selected column of that row
        :param __sql: sql statement
        :param __params: (optional) parameter arguments
        :return: value of first column of rows
        """
        out = self.fetch(__sql, *__params, n=1, row_factory=None)
        return out if not out else out[0]

    def aggregate(self, table_name: str, column: str, agg: str) -> Any:
        """
        utility function to perform an
        aggregate function on a column in a table
        :param table_name: name of table
        :param column: name of column
        :param agg: aggregate function (i.e. SUM, AVG)
        :return: result of aggregate
        """
        stmt = f"SELECT {agg}({column}) FROM {table_name};"
        a = self.fetch_first_value(stmt)
        return a

    def count(self, table_name: str, column: str | None = None) -> int:
        """
        returns the number of rows in a table
        :param table_name: name of table
        :param column: (optional) defaults to None or '*' and returns
        all rows, or specify to select number of all non-null columns
        :return: number of rows
        """
        count = self.aggregate(table_name, "*" if not column else column, "COUNT")
        return count

    def sum(self, table_name: str, column: str) -> float:
        """
        perform sum aggregate on given column in table
        :param table_name: name of table
        :param column: column name
        :return: sum of values in the column
        """
        if not self.column_exists_in_table(table_name, column):
            return -1
        agg = self.aggregate(table_name, column, "SUM")
        return agg

    def avg(self, table_name: str, column: str, precision=2) -> float:
        """
        preform average aggregate on a given column in table
        :param table_name: name of table
        :param column: column name
        :param precision: decimal places in rounding (default: 2)
        :return: average value of values in the column
        """
        if not self.column_exists_in_table(table_name, column):
            return -1
        agg = self.aggregate(table_name, column, "AVG")
        return agg if not agg else round(agg, precision)

    def min(self, table_name: str, column: str) -> Any:
        """
        preform min aggregate on a given column in a table
        :param table_name: name of table
        :param column: column name
        :return: minimum value in column
        """
        if not self.column_exists_in_table(table_name, column):
            return None
        agg = self.aggregate(table_name, column, "MIN")
        return agg

    def max(self, table_name: str, column: str) -> Any:
        """
        preform max aggregate on a given column in a table
        :param table_name: name of table
        :param column: column name
        :return: maximum value in column
        """
        if not self.column_exists_in_table(table_name, column):
            return None
        agg = self.aggregate(table_name, column, "MAX")
        return agg

    def fetch(self,
              __sql: str,
              *__params,
              n: int | None = None,
              row_factory: sql.Row | None = sql.Row
              ) -> None | List[sql.Row] | List[Tuple]:
        """
        fetch statement execution with specificed number of
        rows to fetch
        :param __sql: sql statement
        :param __params: (optional) parameter values
        :param n: number of rows to fetch (default: None = all)
        :param row_factory: set to None to return tuples, otherwise will
        return dict convertible Row objects
        :return: result of fetch
        """
        with self.get_con() as con:
            con.row_factory = row_factory
            cur = con.cursor()
            try:
                cur.execute(__sql, list(__params))
                if n is None:
                    return cur.fetchall()
                elif n == 1:
                    return cur.fetchone()
                else:
                    return cur.fetchmany(n)
            except sql.Error:
                return None

    def execute(self, statement: str, *params, as_transaction: bool = False) -> bool:
        """
        execute an sql statement
        :param statement: sql statement
        :param params: (optional) parameter values
        :param as_transaction: flag of whether to run
        the execution as a transaction
        :return: boolean whether execution was successful
        """
        with self.get_con() as con:
            cur = con.cursor()
            try:
                if as_transaction:
                    cur.execute("BEGIN TRANSACTION;")
                cur.execute(statement, list(params))
                con.commit()
            except sql.Error:
                con.rollback()
                return False
        return True

    def executescript(self, __sql: str) -> bool:
        """
        executes an sql script
        :param __sql: sql script
        :return: boolean whether the execution was successful
        """
        with self.get_con() as con:
            cur = con.cursor()
            try:
                cur.executescript(__sql)
                con.commit()
            except sql.Error as e:
                if self.debug:
                    print(e)
                con.rollback()
                return False
        return True

    def executemany(self, __sql: str, __seq_of_params) -> bool:
        """
        executes an sql statement many times on
        a sequence of parameter values
        :param __sql: sql statement
        :param __seq_of_params: sequence of data
        :return: boolean whether execution was successful
        """
        with self.get_con() as con:
            cur = con.cursor()
            try:
                cur.executemany(__sql, __seq_of_params)
                con.commit()
            except sql.Error as e:
                if self.debug:
                    print(e)
                con.rollback()
                return False
        return True

    def __vacuum(self):
        """utility for vacuuming database"""
        self.execute("VACUUM;")

    def __get_database_name(self):
        """ utility for getting the filename of database """
        _, fname = os.path.split(self.file)
        name, _ = os.path.splitext(fname)
        return name

    def dump(self, out_file: str = None) -> None:
        """
        dump the entire database schema to a
        .sql file
        :param out_file: (optional) name of the destination file
        :return: None
        """
        if not out_file:
            out_file = "%s.sql" % self.__get_database_name()
        dst = open(out_file, 'w', encoding="utf-16")
        with self.get_con() as con:
            lines = '\n'.join(con.iterdump())
            dst.writelines(lines)
        dst.close()

    def export_to_csv(self) -> None:
        """
        exports every table in the database
        to seperate csvs
        :return: None
        """
        for table in self.get_table_names():
            self.export_table_to_csv(table)

    def export_table_to_csv(self, table_name: str, delimeter: str = ',') -> None:
        """
        exports a table in the database to csv format
        :param table_name: name of table
        :param delimeter: csv file delimeter character
        :return: None
        """
        if not self.table_exists(table_name):
            return
        outfile = f"./{self.__get_database_name()}-{table_name}.csv"
        rows = list(map(dict, self.select(table_name)))
        if not len(rows):
            return
        headers = list(rows[0].keys())  # extract column header line
        csv_rows = [list(row.values()) for row in rows]
        with open(outfile, 'w', newline='', encoding='UTF-8') as csv_file:
            writer = csv.writer(csv_file, delimiter=delimeter)
            writer.writerow(headers)
            writer.writerows(csv_rows)
