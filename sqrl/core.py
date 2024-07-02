"""
Oliver 2024
"""

from . import utils
import csv as _csv
import os.path as _op
import sqlite3 as _sql
from typing import Dict, List, Any, Tuple

IN_MEMORY = ":memory:"


class SQL:
    def __init__(
            self,
            filename: str = IN_MEMORY,
            debug: bool = False,
            check_same_thread: bool = True,
            optimize: bool = True,
            foreign_keys: bool = True
    ):
        """
        :param filename: path to database file
        :param debug: flag of whether to echo statements and errors
        :param check_same_thread: flag for the db connection to check if running on the same thread (on by default)
        :param optimize: set journal mode to write ahead log and other optimizations (on by default)
        :param foreign_keys: enables foreign key flag (on by default)
        """
        self.file: str = filename
        self.con: _sql.Connection = _sql.connect(filename, check_same_thread=check_same_thread, cached_statements=True)
        if foreign_keys:
            self.execute("pragma foreign_keys = on;")
        if optimize and self.file != IN_MEMORY:
            # check if WAL already enabled
            with open(filename, "rb") as file:
                file.seek(19)
                write = file.read(1)
                read = file.read(1)
                if not (write == '\x02' and read == '\x02'):
                    self.executescript("""
                       pragma journal_mode = WAL; -- write to sequential write-ahead log, and sync later
                       pragma synchronous = normal;
                       pragma journal_size_limit = 6144000;
                       """)
        self.debug: bool = debug
        self.schema: Dict[str, List[str]] = {}

    def get_table_names(self) -> List[str]:
        """
        returns a list of all table names in the database
        :return: list of strings of table names
        """
        tables = self.fetch(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;",
            row_factory=None
        )
        return tables

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
            table_name = table
            # get all of the current tables columns
            self.schema[table_name] = self.get_column_names(table_name)

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
               row_factory: _sql.Row | None = _sql.Row,
               return_as_dict: bool = False,
               ) -> List[Dict[str, Any]] | Dict[str, Any] | List[Tuple[Any]] | List[_sql.Row] | _sql.Row | Tuple[
        Any]:
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
        :param return_as_dict: returns already converted to dictionayr object
        :return: list of items from select
        """
        # format columns
        if columns:
            col_list = ",".join(columns)
        else:
            col_list = "*"

        # build select statement
        core = f"SELECT {'DISTINCT ' if distinct else ''}{col_list} FROM {table_name}"
        where_chunk = '' if not where else f' WHERE {where}'
        group_by_chunk = '' if not group_by else f' GROUP BY {group_by}'
        having_chunk = '' if not having else f' HAVING {having}'
        order_by_chunk = '' if not order_by else f" ORDER BY {order_by} {'ASC' if asc else 'DESC'}"
        limit_offset_chunk = f" LIMIT {limit} OFFSET {offset};"

        stmt = ''.join([core, where_chunk, group_by_chunk, having_chunk, order_by_chunk, limit_offset_chunk])

        result = self.fetch(stmt, n=fetch, row_factory=row_factory, return_as_dict=return_as_dict)
        return result

    def insert(self,
               table_name: str,
               data: Dict[str, Any],
               replace: bool = False,
               returning: str | None = None) -> bool | List[Tuple[Any]]:
        """
        insert (or replace) new data into a table
        :param table_name: name of table in database
        :param data: a dictionary of column names and the values
        :param replace: flag of whether make it an OR REPLACE statement
        :param returning: optional string input for a returning clause after insertion
        :return: boolean whether execution was successful
        """
        columns, values = utils.process_dict(data)
        core = "INSERT INTO" if not replace else "INSERT OR REPLACE INTO"
        col_list = ','.join(columns)
        val_list = ','.join(['?' for _ in values])
        stmt = f"{core} {table_name} ({col_list}) VALUES ({val_list}){' RETURNING %s' % returning if returning else ''};"
        if self.debug:
            print(stmt)
        res = self.execute(stmt, *values, as_transaction=True, has_return=returning is not None)
        return res

    def update(self,
               table_name: str,
               data: Dict[str, Any],
               where: str = "1 = 1",
               returning: str | None = None) -> bool | List[Tuple[Any]]:
        """
        update data within a given table
        :param table_name: table name to update in
        :param data: dictionary of column names and new values
        :param where: conditional clause for updating (default: 1 = 1 / update everything)
        :param returning: optional string input for a returning clause after update
        :return: boolean whether execution was successful
        """
        columns, values = utils.process_dict(data)

        params = ', '.join([f"{c} = ?" for c in columns])
        stmt = f"UPDATE {table_name} SET {params} WHERE {where}{' RETURNING %s' % returning if returning else ''};"

        res = self.execute(stmt, *values, as_transaction=True, has_return=returning is not None)
        return res

    def delete(self,
               table_name: str,
               where: str,
               returning: str | None = None,
               vacuum: bool = False) -> bool | List[Tuple[Any]]:
        """
        delete from a table based on a given conditional
        :param table_name: name of table in database
        :param where: conditional for which item to delete from table
        :param returning: optional string input for a returning clause after update
        :param vacuum: flag to specify whether to vacuum db after this delete operation
        :return: boolean of whether execution was successful
        """

        stmt = f"DELETE FROM {table_name} WHERE {where}{' RETURNING %s' % returning if returning else ''};"

        success = self.execute(stmt, as_transaction=True, has_return=returning is not None)
        if vacuum and success:
            self.vacuum()
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
        if len(self.schema) == 0:
            self.build_schema()
        table_cols = self.schema.get(table_name, None)
        if table_cols is None:
            return False
        column_found: bool = column in self.schema[table_name]
        return column_found

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
        if self.debug:
            print(stmt)
        a = self.fetch(stmt, n=1, row_factory=None)
        return a

    def count(self, table_name: str, column: str | None = None) -> int | None:
        """
        returns the number of rows in a table
        :param table_name: name of table
        :param column: (optional) defaults to None or '*' and returns
        all rows, or specify to select number of all non-null columns
        :return: number of rows
        """
        count = self.aggregate(table_name, "*" if not column else column, "COUNT")
        return count

    def sum(self, table_name: str, column: str) -> float | None:
        """
        perform sum aggregate on given column in table
        :param table_name: name of table
        :param column: column name
        :return: sum of values in the column
        """
        agg = self.aggregate(table_name, column, "SUM")
        return agg

    def avg(self, table_name: str, column: str, precision=2) -> float | None:
        """
        preform average aggregate on a given column in table
        :param table_name: name of table
        :param column: column name
        :param precision: decimal places in rounding (default: 2)
        :return: average value of values in the column
        """
        agg = self.aggregate(table_name, column, "AVG")
        return agg if agg is None else round(agg, precision)

    def min(self, table_name: str, column: str) -> Any:
        """
        preform min aggregate on a given column in a table
        :param table_name: name of table
        :param column: column name
        :return: minimum value in column
        """
        agg = self.aggregate(table_name, column, "MIN")
        return agg

    def max(self, table_name: str, column: str) -> Any:
        """
        preform max aggregate on a given column in a table
        :param table_name: name of table
        :param column: column name
        :return: maximum value in column
        """
        agg = self.aggregate(table_name, column, "MAX")
        return agg

    def fetch(self,
              __sql: str,
              *__params,
              n: int | None = None,
              row_factory: _sql.Row | None = _sql.Row,
              return_as_dict: bool = False
              ) -> None | List[Dict[str, Any]] | Dict[str, Any] | List[_sql.Row] | List[Tuple] | _sql.Row | Tuple:
        """
        fetch statement execution with specificed number of
        rows to fetch
        :param __sql: sql statement
        :param __params: (optional) parameter values
        :param n: number of rows to fetch (default: None = all)
        :param row_factory: set to None to return tuples, otherwise will
        :param return_as_dict: return already casted to a dictionary object
        return dict convertible Row objects
        :return: result of fetch
        """
        self.con.row_factory = row_factory
        cur = self.con.cursor()
        try:
            cur.execute(__sql, list(__params))
            if n is None:
                res = cur.fetchall()
                if not res:
                    return []
                if return_as_dict:
                    return [dict(x) for x in res]
                if row_factory is None and len(res[0]) == 1:
                    return [x[0] for x in res]
                return res
            elif n == 1:
                res = cur.fetchone()
                if not res:
                    return res
                if return_as_dict:
                    return dict(res)
                if row_factory is None and len(res) == 1:
                    return res[0]
                return res
            else:
                res = cur.fetchmany(n)
                if not res:
                    return []
                if return_as_dict:
                    return [dict(x) for x in res]
                if row_factory is None and len(res[0]) == 1:
                    return [x[0] for x in res]
                return res
        except _sql.Error:
            return None

    def execute(self,
                statement: str,
                *params,
                as_transaction: bool = False,
                has_return: bool = False) -> bool | List[Tuple[Any]]:
        """
        execute an sql statement
        :param statement: sql statement
        :param params: (optional) parameter values
        :param as_transaction: flag of whether to run
        the execution as a transaction
        :param has_return: flag of whether statement returns data
        (e.g. UPDATE BOOKS SET isbn = '1234' WHERE id = 1 RETURNING *;
        :return: boolean whether execution was successful
        """
        cur = self.con.cursor()
        try:
            if as_transaction:
                cur.execute("BEGIN TRANSACTION;")
            cur.execute(statement, list(params))
            out = cur.fetchall() if has_return else True
            self.con.commit()
            return out
        except _sql.Error as e:
            if self.debug:
                print(e)
            self.con.rollback()
            return False

    def executescript(self, __sql: str) -> bool:
        """
        executes an sql script
        :param __sql: sql script
        :return: boolean whether the execution was successful
        """
        cur = self.con.cursor()
        try:
            cur.executescript(__sql)
            self.con.commit()
        except _sql.Error as e:
            if self.debug:
                print(e)
            self.con.rollback()
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
        cur = self.con.cursor()
        cur.execute("BEGIN TRANSACTION;")
        try:
            cur.executemany(__sql, __seq_of_params)
            self.con.commit()
        except _sql.Error as e:
            if self.debug:
                print(e)
            self.con.rollback()
            return False
        return True

    def vacuum(self):
        """utility for vacuuming database"""
        self.execute("VACUUM;")

    def __get_database_name(self):
        """ utility for getting the filename of database """
        if self.file == IN_MEMORY:
            return "memory"
        _, fname = _op.split(self.file)
        name, _ = _op.splitext(fname)
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
        lines = '\n'.join(self.con.iterdump())
        dst.writelines(lines)
        dst.close()

    def export_to_csv(self, delimeter: str = ',') -> None:
        """
        exports every table in the database
        to seperate csvs
        :return: None
        """
        for table in self.get_table_names():
            self.export_table_to_csv(table_name=table, delimeter=delimeter)

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
        rows = [dict(tab) for tab in self.select(table_name)]
        if not len(rows):
            return
        headers = list(rows[0].keys())  # extract column header line
        csv_rows = [list(row.values()) for row in rows]
        with open(outfile, 'w', newline='', encoding='UTF-8') as csv_file:
            writer = _csv.writer(csv_file, delimiter=delimeter)
            writer.writerow(headers)
            writer.writerows(csv_rows)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.con.close()
