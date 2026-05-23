from uop.core import async_db_collection, async_database
from uop.sql.base.table import Table
from uop.meta.schemas import meta
import inspect
from pydantic import BaseModel


class AsyncSQLBaseDatabase(async_database.Database):
    JSON_SUPPORTED = False  # general case
    Table_Class = Table

    def __init__(self, dbname, *schemas, tenant_id=None, **db_credentials):
        self._conn = self._autoconn = None
        # TODO fix this for async. cannot be in __init__
        self._known_tables = set()
        super().__init__(
            dbname,
            *schemas,
            tenant_id=tenant_id,
            **db_credentials,
        )

    async def open_db(self):
        self._known_tables = await self.get_existing_tables()
        await super().open_db()

    async def get_existing_tables(self):
        cursor = await self.connection.cursor()
        await cursor.execute(self.Table_Class().all_tables_string())
        tables = {row[0] for row in await cursor.fetchall()}
        await cursor.close()
        return tables

    async def open_db(self):
        await super().open_db()
        self._known_tables = await self.get_existing_tables()

    async def close_db(self):
        if self._conn:
            await self._conn.close()
            self._conn = None
        if self._autoconn:
            await self._autoconn.close()
            self._autoconn = None

    @property
    def connection(self):
        return self._conn if self.in_long_transaction else self._autoconn

    async def execute_sql(self, clause, params):
        curr = self.connection.cursor()
        await curr.execute(clause, params)
        return curr

    async def set_autocommit(self, connection, autocommit=True):
        await connection.set_autocommit(autocommit)

    async def get_connection(self):
        return self._conn if self.in_long_transaction else self._autoconn

    async def get_cursor(self):
        return await self.connection.cursor()

    async def start_long_transaction(self):
        await super().start_long_transaction()

    async def end_long_transaction(self):
        await super().end_long_transaction()

    async def db_commit(self):
        await self.connection.commit()

    async def db_abort(self):
        await self.connection.rollback()

    def row_as_dict(self, row):
        return row

    async def get_managed_collection(self, name, schema):
        existing = self.collections.get(name)
        if existing:
            return existing
        res = AsyncSQLBaseCollection(self, name, schema)
        await self.ensure_table_exists(res)
        return res

    async def ensure_table_exists(self, collection):
        if collection._table.name not in self._known_tables:
            await collection.create_table()
            self._known_tables.add(collection._table.name)

    async def execute_ddl(self, clause, params):
        # TODO research and fix for maybe special conn for DDL
        cursor = await self._autoconn.execute(clause, params)
        await cursor.close()


class AsyncSQLBaseCollection(async_db_collection.DBCollection):
    Table_Class = Table

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

    async def create_table(self):
        clause, vals = self._table.table_creation_string(), {}
        await self._db.execute_ddl(clause, vals)

    def table_from_schema(self, name, schema, supports_json=False):
        if isinstance(schema, dict):
            schema = meta.MetaClass(**schema)
        if isinstance(schema, meta.MetaClass):
            name = schema.name
            uop_types = schema.uop_types()
        elif inspect.isclass(schema) and issubclass(schema, BaseModel):
            uop_types = meta.extract_uop_field_types(schema)
        return self._db.Table_Class(name, uop_types, self._supports_json)

    async def _execute(self, clause, params):
        # TODO serialization not worknig 5/16 so likely the setup to do it is wrong
        serialized = self._table.json_serialize(params)
        curr = await self._db.execute_sql(clause, serialized)
        return curr

    async def _execute_only(self, clause, params):
        curr = await self._execute(clause, params)
        await curr.close()

    async def _fetch_one(self, clause, parats):
        curr = await self._execute(clause, parats)
        res = await curr.fetchone() if curr.rowcount else None
        await curr.close()
        return res

    def process_row(self, row):
        if row:
            return self._table.json_deserialize(self._db.row_as_dict(row))


    async def _fetch_all(self, clause, params):
        curr = await self._execute(clause, params)
        res = await curr.fetchall() if curr.rowcount else []
        await curr.close()
        return [self.process_row(row) for row in res]

    async def count(self, criteria):
        clause, vals = self._table.count_string(criteria)
        return await self._fetch_one(clause, vals)

    async def insert(self, **data):
        clause = self._table.insert_string()
        await self._execute_only(clause, data)

    async def update(self, criteria, mods):
        clause, vals = self._table.update_string(criteria, mods)
        return await self._execute_only(clause, vals)

    async def update_one(self, criteria, mods):
        if isinstance(criteria, str):
            criteria = {"id": criteria}
        return await self.update(criteria, mods)

    async def remove(self, criteria):
        if isinstance(criteria, str):
            criteria = {"id": criteria}
        clause, vals = self._table.delete_string(criteria)
        return await self._execute_only(clause, vals)

    async def get(self, an_id):
        clause, vals = self._table.get_by_id_string(an_id)
        return await self._fetch_one(clause, vals)

    async def find(
        self, criteria=None, only_cols=None, order_by=None, limit=None, ids_only=False
    ):
        only_one = limit == 1
        clause, vals = self._table.select_string(criteria, only_cols, order_by, limit)
        fetcher = self._fetch_one if only_one else self._fetch_all
        res = await fetcher(clause, vals)
        if only_cols and len(only_cols) == 1:
            return [row[only_cols[0]] for row in res]
        return res

    async def find_one(self, criteria, only_cols=None):
        return await self.find(criteria, only_cols=only_cols, limit=1)

    async def exists(self, criteria):
        return await self.find_one(criteria)
