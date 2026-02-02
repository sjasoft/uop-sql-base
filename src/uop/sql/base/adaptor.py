from uop.core import db_collection, database
from uop.sql.base.table import Table
from uop.meta.schemas import meta
import inspect
from pydantic import BaseModel


class SQLBaseCollection(db_collection.DBCollection):
    def __init__(self, db, name, schema):
        """Creates a table object from the schema and sets
        up collection for DBAPI style database interfaces

        Args:
            db (_type_): The database the collection is in
            schema (_type_): Either a MetaClass or pydantic class.
            name and column names and types can be extracted from either

        """
        self._db = db
        self._supports_json = db.JSON_SUPPORTED
        self._table = self.table_from_schema(name, schema)

        super().__init__(self._table)

    def create_table(self):
        clause = self._table.table_creation_string()
        self._db.execute_ddl(clause, {})

    def process_row(self, row):
        if row:
            return self._table.json_deserialize(self._db.row_as_dict(row))

    def table_from_schema(self, name, schema, supports_json=False):
        if isinstance(schema, dict):
            schema = meta.MetaClass(**schema)
        if isinstance(schema, meta.MetaClass):
            name = schema.name
            uop_types = schema.uop_types()
        elif inspect.isclass(schema) and issubclass(schema, BaseModel):
            uop_types = meta.extract_uop_field_types(schema)
        return self._db.Table_Class(name, uop_types, self._supports_json)

    def _execute(self, clause, params):
        serialized = self._table.json_serialize(params)
        curr = self._db.execute_sql(clause, serialized)
        return curr

    def _fetch_one(self, clause, params):
        curr = self._execute(clause, params)
        res = curr.fetchone() if curr.rowcount else None
        curr.close()
        return self.process_row(res)
    
    def _execute_only(self, clause, params):
        curr = self._execute(clause, params)
        curr.close()

    def _fetch_all(self, clause, params):
        curr = self._execute(clause, params)
        res = curr.fetchall() if curr.rowcount else []
        curr.close()
        return [self.process_row(row) for row in res]

    def count(self, criteria):
        clause, vals = self._table.count_string(criteria)
        return self._fetch_one(clause, vals)

    def insert(self, **data):
        clause = self._table.insert_string()
        curr = self._execute(clause, data)
        curr.close()

    def update(self, criteria, mods):
        clause, vals = self._table.update_string(criteria, mods)
        return self._execute_only(clause, vals)
    
    def update_one(self, criteria, mods):
        if isinstance(criteria, str):
            criteria = {'id': criteria}
        return self.update(criteria, mods)


    def remove(self, criteria):
        if isinstance(criteria, str):
            criteria = {'id': criteria}
        clause, vals = self._table.delete_string(criteria)
        return self._execute_only(clause, vals)

    def get(self, an_id):
        clause, vals = self._table.get_by_id_string(an_id)
        return self._fetch_one(clause, vals)

    def find(
        self, criteria=None, only_cols=None, order_by=None, limit=None, ids_only=False
    ):
        only_one = limit == 1
        clause, vals = self._table.select_string(criteria, only_cols, order_by, limit)
        fetcher = self._fetch_one if only_one else self._fetch_all
        res = fetcher(clause, vals)
        if only_cols and len(only_cols) == 1:
            res = list(res)
            return [row[only_cols[0]] for row in res]
        return res

    def find_one(self, criteria, only_cols=None):
        return self.find(criteria, only_cols=only_cols, limit=1)

    def exists(self, criteria):
        return self.find_one(criteria)


class SQLBaseDatabase(database.Database):
    JSON_SUPPORTED = False  # general case
    Table_Class = Table

    def __init__(
        self, dbname, *schemas, tenant_id=None, db_brand="sqlbase", **db_credentials
    ):
        self._conn = self._autoconn = None
        super().__init__(
            dbname,
            *schemas,
            tenant_id=tenant_id,
            **db_credentials,
        )
        self._known_tables = set()

    def close_db(self):
        if self._conn:
            self._conn.close()
            self._conn = None
        if self._autoconn:
            self._autoconn.close()
            self._autoconn = None

    @property
    def connection(self):
        return self._conn if self.in_long_transaction else self._autoconn

    def execute_sql(self, clause, params):
        curr = self.connection.cursor()
        curr.execute(clause, params)
        return curr

    def set_autocommit(self, connection, autocommit=True):
        connection.autocommit = autocommit

    def get_connection(self):
        return self._conn

    def get_cursor(self):
        return self.connection.cursor()

    def start_long_transaction(self):
        #elf.set_autocommit(self.connection, False)
        super().start_long_transaction()

    def end_long_transaction(self):
        #elf.set_autocommit(self.connection, True)
        super().end_long_transaction()

    def db_commit(self):
        self.connection.commit()

    def db_abort(self):
        self.connection.rollback()

    def row_as_dict(self, row):
        return row

    def get_existing_tables(self):
        return set()

    def get_managed_collection(self, name, schema):
        existing = self._collections.get(name)
        if existing:
            return existing
        res = SQLBaseCollection(self, name, schema)
        self.ensure_table_exists(res)
        return res

    def ensure_table_exists(self, collection):
        if collection._table.name not in self._known_tables:
            collection.create_table()
            self._known_tables.add(collection._table.name)

    def execute_ddl(self, clause, params):
        # TODO research and fix for maybe special conn for DDL
        cursor = self._autoconn.execute(clause, params)
        cursor.close()
        

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
        if self._autoconn:
            self._autoconn.close()
            self._autoconn = None
