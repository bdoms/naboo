import asyncio
from datetime import datetime, date, time
import os
from uuid import UUID

import asyncpg
import pytest

from naboo import (Database, Field, Query, Model, ArrayField, BooleanField, ByteField, CharField,
    DateField, DateTimeField, FloatField, ForeignKeyField, IntField, TextField, TimeField, UUIDField)
from naboo.db import MAX_FIELD_LENGTH


async def setup_db():
    await Database.startup(os.environ['POSTGRES_DB'], os.environ['POSTGRES_USER'],
        os.environ['POSTGRES_PASSWORD'])


async def setup_table(table_class):
    await setup_db()
    db_conn = await Database.connect()
    await table_class.createTable(db_conn)
    await db_conn.close()


async def teardown_table(table_class):
    await setup_db()
    db_conn = await Database.connect()
    await table_class.dropTable(db_conn)
    await db_conn.close()


@pytest.fixture
async def conn():
    # see https://medium.com/@geoffreykoh/fun-with-fixtures-for-database-applications-8253eaf1a6d
    await setup_db()

    db_conn = await Database.connect()

    yield db_conn

    await db_conn.close()


def test_field_create():
    # this is ann abstract base class, so we have to manually set a type
    field = Field()
    field.db_type = 'int'

    # safety checks for whitespace
    with pytest.raises(ValueError):
        field.create('testtable', 'test column')

    with pytest.raises(ValueError):
        field.create('test table', 'testcolumn')

    # double quotes
    with pytest.raises(ValueError):
        field.create('testtable', 'test"column')

    with pytest.raises(ValueError):
        field.create('test"table', 'testcolumn')

    # single quotes
    with pytest.raises(ValueError):
        field.create('testtable', "test'column")

    with pytest.raises(ValueError):
        field.create("test'table", 'testcolumn')

    col, constraint = field.create('testtable', 'id')
    assert col == f'"id" {field.db_type} PRIMARY KEY'
    assert constraint is None

    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type} NOT NULL'
    assert constraint is None

    field = Field(null=True, default='test default', unique=True)
    field.db_type = 'int'

    col, constraint = field.create('testtable', 'id')
    assert col == f'"id" {field.db_type} DEFAULT test default PRIMARY KEY'
    assert constraint is None

    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type} DEFAULT test default UNIQUE'
    assert constraint is None


def test_array_field():

    with pytest.raises(TypeError):
        ArrayField('foo', default=None)

    with pytest.raises(TypeError):
        ArrayField(str, default='foo')

    with pytest.raises(TypeError):
        ArrayField(str, default=[1])

    field = ArrayField(str, default=['abc'])

    with pytest.raises(TypeError):
        field.create('testtable', 'id')

    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.field_type} DEFAULT [\'abc\'] NOT NULL'
    assert constraint is None


def test_boolean_field():

    with pytest.raises(TypeError):
        BooleanField(default='')

    field = BooleanField(default=False)

    with pytest.raises(TypeError):
        field.create('testtable', 'id')

    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type} DEFAULT False NOT NULL'
    assert constraint is None


def test_byte_field():

    with pytest.raises(TypeError):
        ByteField(default=False)

    with pytest.raises(TypeError):
        ByteField(default='abc')

    with pytest.raises(ValueError):
        ByteField(default=b"ab'c")

    with pytest.raises(ValueError):
        ByteField(default=b'ab\\c')

    field = ByteField(default=b'abc')

    with pytest.raises(TypeError):
        field.create('testtable', 'id')

    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type} DEFAULT \'abc\' NOT NULL'
    assert constraint is None

    field = ByteField()
    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type} NOT NULL'
    assert constraint is None


def test_char_field():

    with pytest.raises(TypeError):
        CharField(default=False)

    with pytest.raises(ValueError):
        CharField(default="ab'c")

    with pytest.raises(ValueError):
        CharField(default='ab\\c')

    field = CharField(default='abc')

    with pytest.raises(TypeError):
        field.create('testtable', 'id')

    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type}({MAX_FIELD_LENGTH}) DEFAULT \'abc\' NOT NULL'
    assert constraint is None

    field = CharField(max_length=100)
    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type}(100) NOT NULL'
    assert constraint is None


def test_date_field():

    with pytest.raises(TypeError):
        DateField(default=False)

    field = DateField()

    with pytest.raises(TypeError):
        field.create('testtable', 'id')

    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type} NOT NULL'
    assert constraint is None

    field = DateField(default=date(2020, 1, 1))
    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type} DEFAULT 2020-01-01 NOT NULL'
    assert constraint is None


def test_date_time_field():

    with pytest.raises(TypeError):
        DateTimeField(default=False)

    field = DateTimeField(auto_now=False)

    with pytest.raises(TypeError):
        field.create('testtable', 'id')

    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type} NOT NULL'
    assert constraint is None

    field = DateTimeField(auto_now=True)
    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type} DEFAULT CURRENT_TIMESTAMP NOT NULL'
    # FUTURE: test this constraint trigger more thoroughly
    assert constraint is not None

    field = DateTimeField(default=datetime(2020, 1, 1))
    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type} DEFAULT 2020-01-01 00:00:00.000000 NOT NULL'
    assert constraint is None


def test_int_field():

    with pytest.raises(TypeError):
        IntField(default='')

    field = IntField(default=0)

    with pytest.raises(TypeError):
        field.create('testtable', 'id')

    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type} DEFAULT 0 NOT NULL'
    assert constraint is None


def test_float_field():

    with pytest.raises(TypeError):
        FloatField(default='')

    field = FloatField(default=0.0)

    with pytest.raises(TypeError):
        field.create('testtable', 'id')

    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type} DEFAULT 0.0 NOT NULL'
    assert constraint is None


def test_uuid_field():

    with pytest.raises(TypeError):
        UUIDField(default='')

    default = UUID('1234' * 8)
    field = UUIDField(default=default)

    col, constraint = field.create('testtable', 'id')
    assert col == f'"id" {field.db_type} DEFAULT {default} PRIMARY KEY'
    assert constraint is None

    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type} DEFAULT {default} NOT NULL'
    assert constraint is None


def test_text_field():

    with pytest.raises(TypeError):
        TextField(default=False)

    with pytest.raises(ValueError):
        TextField(default="ab'c")

    with pytest.raises(ValueError):
        TextField(default='ab\\c')

    field = TextField(default='abc')

    with pytest.raises(TypeError):
        field.create('testtable', 'id')

    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type} DEFAULT \'abc\' NOT NULL'
    assert constraint is None


def test_time_field():

    with pytest.raises(TypeError):
        TimeField(default=False)

    field = TimeField()

    with pytest.raises(TypeError):
        field.create('testtable', 'id')

    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type} NOT NULL'
    assert constraint is None

    field = TimeField(default=time(0, 0, 0))
    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type} DEFAULT 00:00:00.000000 NOT NULL'
    assert constraint is None


def test_foreign_key_field():

    class TestModel(Model):
        id = UUIDField()

    with pytest.raises(TypeError):
        ForeignKeyField(TestModel, default='')

    default = UUID('1234' * 8)
    field = ForeignKeyField(TestModel, default=default)

    with pytest.raises(TypeError):
        field.create('testtable', 'id')

    col, constraint = field.create('testtable', 'foo')
    assert col == f'"foo" {field.db_type} DEFAULT {default} NOT NULL'
    # FUTURE: test this constraint more thoroughly
    assert constraint is not None

    # test that using a string works after the model already exists
    field = ForeignKeyField('TestModel')
    field.create('testtable2', 'foo2')

    # test that using a string works before the model exists
    field = ForeignKeyField('TestLazyInit')

    class TestLazyInit(Model):
        id = UUIDField()

    field.create('testtable3', 'foo3')


class QueryTest(Model):
    id = UUIDField()
    name = CharField(null=True)


class TestQuery:

    @classmethod
    def setup_class(cls):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(setup_table(QueryTest))

    @classmethod
    def teardown_class(cls):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(teardown_table(QueryTest))

    async def test_query(self, conn):
        alias = 'testalias'
        q = Query(conn, QueryTest, alias=alias)

        sql = f'SELECT * FROM public."query_test" AS "{alias}"'
        assert q.sql == sql

        # can't close a group before you start
        with pytest.raises(RuntimeError):
            q.end_logic()

        q.start_logic()

        sql += ' WHERE ('
        assert q.sql == sql

        # these should fail because the logic was opened but not closed
        with pytest.raises(RuntimeError):
            await q.all()

        with pytest.raises(RuntimeError):
            await q.count()

        with pytest.raises(RuntimeError):
            await q.first()

        # can't close an empty group
        with pytest.raises(RuntimeError):
            q.end_logic()

        with pytest.raises(ValueError):
            q.where('doesntexist', '=', 'foo')

        with pytest.raises(ValueError):
            q.where('name', '=', None)

        with pytest.raises(ValueError):
            q.where('name', 'IS', 'foo')

        q.where('name', '=', 'foo')

        sql += f'"{alias}"."name" = $1'
        assert q.sql == sql

        sql += ')'
        q.end_logic()

        assert q.sql == sql

        q.add_logic('OR')

        sql += ' OR'
        assert q.sql == sql

        subquery = Query(conn, QueryTest, columns=('id',), alias='s1').where('name', '=', 'foo').where(
            'name', '=', 'name', parent_query=q)

        sub_sql = f'SELECT "id" FROM {QueryTest.schema_table} AS "s1" WHERE "s1"."name" = $1 '
        sub_sql += f'AND "s1"."name" = "{alias}"."name"'
        assert subquery.sql == sub_sql

        # the generated sql has different numbers because of pre-existing parameters on the parent query
        q.exists(subquery)
        sql += f' EXISTS ({sub_sql.replace("$1", "$2")})'
        assert q.sql == sql

        with pytest.raises(ValueError):
            q.order_by('doesntexist')

        q.order_by('name')

        assert q.order_by_sql == ' ORDER BY "name" ASC'

        # can apply multiple orderings
        q.order_by('id', direction='DESC')

        assert q.order_by_sql == ' ORDER BY "name" ASC, "id" DESC'

        # has to be int
        with pytest.raises(TypeError):
            q.limit('foo')

        # has to be above 0
        with pytest.raises(ValueError):
            q.limit(0)

        # and can't be stupid high
        with pytest.raises(ValueError):
            q.limit(1000000000)

        q.limit(10)
        assert q.limit_sql == ' LIMIT 10'

        # can't call it twice
        with pytest.raises(RuntimeError):
            q.limit(10)

        # has to be int
        with pytest.raises(TypeError):
            q.offset('foo')

        # has to be 0 or above
        with pytest.raises(ValueError):
            q.offset(-1)

        q.offset(10)
        assert q.offset_sql == ' OFFSET 10'

        # can't call it twice
        with pytest.raises(RuntimeError):
            q.offset(10)

        # make sure these things actually get applied
        assert q.order_by_sql in q.sql
        assert q.limit_sql in q.sql
        assert q.offset_sql in q.sql

        # we don't actually care about results here, just that the functions run properly
        rows = await q.all()
        assert len(rows) == 0

        count = await q.count()
        assert count == 0

        first = await q.first()
        assert first is None

    async def test_in_list(self, conn):
        q = Query(conn, QueryTest)
        q = q.where('name', 'IN', ['foo', 'bar'])

        sql = f'SELECT * FROM {QueryTest.schema_table} WHERE "name" = Any($1)'
        assert q.sql == sql

    async def test_not_in_list(self, conn):
        q = Query(conn, QueryTest)
        q = q.where('name', 'NOT IN', ['foo', 'bar'])

        sql = f'SELECT * FROM {QueryTest.schema_table} WHERE "name" != Any($1)'
        assert q.sql == sql

    async def test_in_subquery(self, conn):
        subq = Query(conn, QueryTest, columns=('name',))
        subq.where('id', '=', '70211a2d-9715-45bb-9f91-033cc1b0b6b0')

        q = Query(conn, QueryTest)
        q.where('name', 'IN', subq)

        subq_sql = f'SELECT "name" FROM {QueryTest.schema_table} WHERE "id" = $1'
        sql = f'SELECT * FROM {QueryTest.schema_table} WHERE "name" IN ({subq_sql})'
        assert q.sql == sql

    async def test_not_in_subquery(self, conn):
        subq = Query(conn, QueryTest, columns=('name',))
        subq.where('id', '=', '70211a2d-9715-45bb-9f91-033cc1b0b6b0')

        q = Query(conn, QueryTest)
        q.where('name', 'IN', subq)

        subq_sql = f'SELECT "name" FROM {QueryTest.schema_table} WHERE "id" = $1'
        sql = f'SELECT * FROM {QueryTest.schema_table} WHERE "name" IN ({subq_sql})'
        assert q.sql == sql


class ModelTest(Model):
    id = UUIDField()
    name = CharField(null=True)


class TestModel:

    @classmethod
    def setup_class(cls):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(setup_table(ModelTest))

    @classmethod
    def teardown_class(cls):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(teardown_table(ModelTest))

    async def test_query(self, conn):
        # this tests creating with only default values
        record = await ModelTest.create(conn)

        # bad id
        with pytest.raises(asyncpg.exceptions.DataError):
            await ModelTest.get(conn, 'foo')

        # id not found
        result = await ModelTest.get(conn, 'abcd' * 8)
        assert result is None

        # legit, works
        result = await ModelTest.get(conn, record['id'])
        assert result['id'] == record['id']

        # no fields to update
        with pytest.raises(ValueError):
            await ModelTest.update(conn, 'foo')

        # can't update id
        with pytest.raises(KeyError):
            await ModelTest.update(conn, 'foo', id='foo')

        # bad id
        with pytest.raises(asyncpg.exceptions.DataError):
            await ModelTest.update(conn, 'foo', name='foo')

        # id not found
        result = await ModelTest.update(conn, 'abcd' * 8, name='foo')
        assert result is None

        # bad id
        with pytest.raises(asyncpg.exceptions.DataError):
            await ModelTest.delete(conn, 'foo')

        # id not found
        result = await ModelTest.delete(conn, 'abcd' * 8)
        assert result == 0

        # legit, works
        result = await ModelTest.delete(conn, record['id'])
        assert result == 1

        # now test with a name that tries to do a sql injection
        sql = "name'); DELETE FROM model_test; --"
        record1 = await ModelTest.create(conn, name=sql)
        assert record1['name'] == sql

        name = 'foo'
        record2 = await ModelTest.create(conn, name=name)
        assert record2['name'] == name

        record2 = await ModelTest.update(conn, record2['id'], name=sql)
        assert record2['name'] == sql

        results = await ModelTest.select(conn).all()
        assert len(results) == 2 # NOQA: PLR2004

        result = await ModelTest.delete_where(conn, 'id', '=', [record1['id'], record2['id']])
        assert result == 2 # NOQA: PLR2004
