import asyncio
import base64
from contextlib import asynccontextmanager
from datetime import datetime, date, time
import hashlib
import inspect
from uuid import UUID

import asyncpg

MAX_FIELD_LENGTH = 1024 * 1024 # 1 MB
MAX_LABEL_LENGTH = 63
MAX_LIMIT = 10000
MAX_SUBQUERY_ARGS = 9


# NOTE that a class with only classmethods like this and changing the class properties is effectively a singleton
class Database:

    pool = None

    @classmethod
    async def startup(cls, name, user, password, host='localhost', port='5432'):

        # see here for connection arguments: https://github.com/MagicStack/asyncpg/blob/master/asyncpg/connection.py
        cls.pool = await asyncpg.create_pool(host=host, port=port, user=user, password=password, database=name)

    @classmethod
    async def shutdown(cls):
        # we have to enforce a timeout externally ourselves - this is the recommended way in the docs
        try:
            await asyncio.wait_for(cls.pool.close(), timeout=1)
        except asyncio.TimeoutError:
            cls.pool.terminate()

    @classmethod
    async def connect(cls):
        return await cls.pool.acquire()

    @classmethod
    @asynccontextmanager
    async def connection(cls):
        conn = await cls.pool.acquire()
        try:
            yield conn
        finally:
            await conn.close()

    @classmethod
    async def dropTables(cls, conn):
        raise NotImplementedError

    @classmethod
    def labelName(cls, label_name):
        # if the name is too long then hash it, but we'd prefer to not in order to keep it readable
        # NOTE that 63 chars is the postgres default for label name max length
        if len(label_name) > MAX_LABEL_LENGTH:
            # NOTE that this produces output that's 64 characters long, which postgres will truncate
            # but the collision metrics on 63 vs 64 is so low in practice it's not worth caring about
            label_name = hashlib.sha256(label_name).hexdigest()
        return label_name


class Field:

    def __init__(self, null=False, default=None, unique=False) -> None:
        self.null = null
        self.default = default
        self.unique = unique

    @property
    def field_type(self):
        return self.db_type

    @classmethod
    def validateName(cls, s, allowed='_'):
        if '.' in allowed and '.' in s:
            # this is a table, where we can allow quotes around the table name
            schema, table = s.split('.', 1)
            if table.startswith('"') and table.endswith('"'):
                # reconstruct without the surrounding quotes for checking
                s = f'{schema}.{table[1:-1]}'

        return all(char.isalnum() or char in allowed for char in s)

    def constraint(self, table_name, col_name): # NOQA: ARG002
        return None

    def create(self, table_name, col_name):
        # NOTE: dot/period is allowed for tables so that schemas can be included
        if not self.validateName(table_name, allowed='._') or not self.validateName(col_name):
            raise ValueError('Invalid table or column name: ' + table_name + ' ' + col_name)

        col = '"' + col_name + '" ' + self.field_type

        if self.default is not None:
            # wrapping default in a string means that it should also work here for bools, ints, etc.
            col += ' DEFAULT ' + str(self.default)

        if col_name == 'id':
            if self.field_type not in ('uuid', 'int'):
                raise TypeError('Field type is not allowed for primary keys: ' + self.field_type)

            if isinstance(self, ForeignKeyField):
                raise TypeError('Foreign keys are not allowed as primary keys')

            col += ' PRIMARY KEY'
        else:
            if not self.null:
                col += ' NOT NULL'

            if self.unique:
                col += ' UNIQUE'

        constraint = self.constraint(table_name, col_name)
        return col, constraint


class ArrayField(Field):

    db_type = 'array'

    # note that for "timestamp" the "without time zone" is implied
    SUPPORTED_TYPES = { # NOQA: RUF012
        'boolean': bool,
        'date': date,
        'float': float,
        'integer': int,
        'text': str,
        'time': time,
        'timestamp': datetime,
        'uuid': UUID
        # 'varchar' # FUTURE: this is going to take extra work to support any value for max length
    }

    def __init__(self, array_type, default=None, **kwargs) -> None:
        sub_type = self.SUPPORTED_TYPES.get(array_type)
        if not sub_type:
            raise TypeError('Invalid array type: ' + array_type)

        if default is not None:
            if not isinstance(default, list):
                raise TypeError('Invalid default type: ' + str(type(default)))

            for item in default:
                if item is not None and not isinstance(item, sub_type):
                    raise TypeError('Invalid default item type: ' + str(type(item)))

        self.array_type = array_type

        super().__init__(default=default, **kwargs)


    @property
    def field_type(self):
        return self.array_type + '[]'


class BooleanField(Field):

    db_type = 'boolean'

    def __init__(self, default=None, **kwargs) -> None:
        if default is not None and not isinstance(default, bool):
            raise TypeError('Invalid default type: ' + str(type(default)))

        super().__init__(default=default, **kwargs)


class CharField(Field):

    db_type = 'varchar'

    def __init__(self, max_length=MAX_FIELD_LENGTH, default=None, **kwargs) -> None:
        # default needs to wrapped in single quotes
        if default is not None:
            if not isinstance(default, str):
                raise TypeError('Invalid default type: ' + str(type(default)))

            # NOTE: in theory we could escape these below, but not sure it's safe
            # probably need some other method to ensure there aren't any crazy tricks here
            # after way too much investigation it's unclear if there is a good method to allow this
            if "'" in default or '\\' in default:
                raise ValueError('Single quotes and backslashes are not allowed in default values: ' + default)
                # escape bad characters
                # default = default.replace("'", "''").replace('\', '\\')

            # wrap it in quotes
            default = "'" + default + "'"

        super().__init__(default=default, **kwargs)
        self.max_lenth = max_length

    @property
    def field_type(self):
        return self.db_type + '(' + str(self.max_lenth) + ')'


class DateField(Field):

    db_type = 'date'

    def __init__(self, default=None, **kwargs) -> None:
        if default is not None:
            if isinstance(default, date):
                default = default.strftime('%Y-%m-%d')
            else:
                raise TypeError('Invalid default type: ' + str(type(default)))

        super().__init__(default=default, **kwargs)


class DateTimeField(Field):

    db_type = 'timestamp without time zone'

    def __init__(self, auto_now_add=False, auto_now=False, default=None, **kwargs) -> None:
        if auto_now_add or auto_now:
            if default is not None:
                raise ValueError('`default` must not be defined when `auto_now` or `auto_now_add` is True')

            default = 'CURRENT_TIMESTAMP'
        elif default is not None:
            if isinstance(default, datetime):
                # NOTE that this assumes the time is UTC already
                default = default.strftime('%Y-%m-%d %H:%M:%S.%f')
            else:
                raise TypeError('Invalid default type: ' + str(type(default)))

        super().__init__(default=default, **kwargs)

        self.auto_now_add = auto_now_add
        self.auto_now = auto_now

    def constraint(self, table_name, col_name):
        if not self.validateName(table_name, allowed='._') or not self.validateName(col_name):
            raise ValueError('Invalid table or column name: ' + table_name + ' ' + col_name)

        # create a function and trigger for this column - possible to do with one function but very ugly, see
        # https://dba.stackexchange.com/questions/127787/trigger-function-taking-column-names-as-parameters-to-modify-the-row

        sql = None
        if self.auto_now:
            full_name = table_name.replace('.', '_').replace('"', '') + '_' + col_name
            function_name = Database.labelName('auto_now_function_' + full_name)

            sql = f'CREATE OR REPLACE FUNCTION "{function_name}"() RETURNS TRIGGER AS $$\n' \
                + 'BEGIN\n' \
                + f'NEW."{col_name}" = NOW();\n' \
                + 'RETURN NEW;\n' \
                + 'END;\n' \
                + "$$ language 'plpgsql';\n"

            # finally, need a trigger that calls the function on update:
            trigger_name = Database.labelName('auto_now_trigger_' + full_name)

            sql += f'CREATE TRIGGER "{trigger_name}" BEFORE UPDATE ON {table_name} ' \
                + f'FOR EACH ROW EXECUTE PROCEDURE "{function_name}"();'

        return sql


class FloatField(Field):

    db_type = 'float'

    def __init__(self, default=None, **kwargs) -> None:
        if default is not None and not isinstance(default, float):
            raise TypeError('Invalid default type: ' + str(type(default)))

        super().__init__(default=default, **kwargs)


class ForeignKeyField(Field):

    def __init__(self, model_class, default=None, **kwargs) -> None:
        if model_class.id.db_type == 'uuid':
            if default is not None and not isinstance(default, UUID):
                raise TypeError('Invalid default type: ' + str(type(default)))
        elif model_class.id.db_type == 'int':
            if default is not None and not isinstance(default, int):
                raise TypeError('Invalid default type: ' + str(type(default)))
        else:
            # FUTURE: in theory strings/charvars could work here too
            raise NotImplementedError

        super().__init__(default=default, **kwargs)
        self.model_class = model_class

        # we can support either int or uuid by dynamically using the other model's type
        self.db_type = model_class.id.db_type

    def constraint(self, table_name, col_name): # NOQA: ARG002
        if not self.validateName(col_name):
            raise ValueError('Invalid column name: ' + col_name)

        # FUTURE: be able to disable "ON DELETE CASCADE"
        name = Database.labelName(col_name + '_fkey')
        return f'CONSTRAINT "{name}" FOREIGN KEY("{col_name}") REFERENCES {self.model_class.schema_table}(id) ' \
            + 'ON DELETE CASCADE'


class IntField(Field):

    db_type = 'integer'

    def __init__(self, default=None, **kwargs) -> None:
        if default is not None and not isinstance(default, int):
            raise TypeError('Invalid default type: ' + str(type(default)))

        super().__init__(default=default, **kwargs)


class TextField(Field):

    db_type = 'text'

    def __init__(self, default=None, **kwargs) -> None:
        # default needs to wrapped in single quotes
        if default is not None:
            if not isinstance(default, str):
                raise TypeError('Invalid default type: ' + str(type(default)))

            # NOTE: in theory we could escape these below, but not sure it's safe
            # probably need some other method to ensure there aren't any crazy tricks here
            # after way too much investigation it's unclear if there is a good method to allow this
            if "'" in default or '\\' in default:
                raise ValueError('Single quotes and backslashes are not allowed in default values: ' + default)
                # escape bad characters
                # default = default.replace("'", "''").replace('\', '\\')

            # wrap it in quotes
            default = "'" + default + "'"

        super().__init__(default=default, **kwargs)


class TimeField(Field):

    db_type = 'time without time zone'

    def __init__(self, default=None, **kwargs) -> None:
        if default is not None:
            if isinstance(default, time):
                # NOTE that this assumes the time is UTC already
                default = default.strftime('%H:%M:%S.%f')
            else:
                raise TypeError('Invalid default type: ' + str(type(default)))

        super().__init__(default=default, **kwargs)


class UUIDField(Field):

    db_type = 'uuid'

    def __init__(self, default='gen_random_uuid()', **kwargs) -> None:
        if default is not None and default != 'gen_random_uuid()' and not isinstance(default, UUID):
            raise TypeError('Invalid default type: ' + str(type(default)))

        super().__init__(default=default, **kwargs)

    @classmethod
    def convert(cls, value):
        return base64.urlsafe_b64encode(value.bytes).rstrip(b'=').decode()


class Query:

    # FUTURE: should we also support "is distinct from" or "between" here?
    # https://www.postgresql.org/docs/13/functions-comparison.html
    DIRECTIONS = ('ASC', 'DESC')
    FUNCTIONS = ('LOWER', 'UPPER')
    LOGICAL = ('AND', 'OR')
    OPERATORS = ('=', '!=', '<', '>', '<=', '>=', 'LIKE', 'ILIKE') # 'IN', '%', '*', '!'
    IS_OPERATORS = ('IS', 'IS NOT')
    IS_VALUES = ('NULL', 'TRUE', 'FALSE', 'UNKOWN')
    # FUTURE: should we alias alternatives like `=>` and `=<` ? better to just have one way or allow either?

    def __init__(self, conn, model_class, columns=None, alias=None) -> None:
        # FUTURE: support selecting specific columns here - note that the count function below will have to be modified
        self.conn = conn
        self.model_class = model_class
        self.args = []
        self.logic_level = 0
        self.alias = alias
        self.order_by_sql = ''
        self.limit_sql = ''
        self.offset_sql = ''

        self._sql = 'SELECT '
        if columns:
            for column in columns:
                self._check_col(column)

            self._sql += ', '.join(['"' + column + '"' for column in columns])
        else:
            self._sql += '*'

        self._sql += f' FROM {model_class.schema_table}'

        if alias:
            self._sql += f' AS "{alias}"'

    @property
    def sql(self):
        # apply all the ending logic as needed
        sql = self._sql

        if self.order_by_sql:
            sql += self.order_by_sql

        if self.limit_sql:
            sql += self.limit_sql

        if self.offset_sql:
            sql += self.offset_sql

        return sql

    def _check_col(self, name):
        fields = self.model_class.fields
        if name not in fields:
            raise ValueError(f'Unknown field {name}')

    def start_logic(self):
        if ' WHERE ' not in self._sql:
            self._sql += ' WHERE'

        self._sql += ' ('
        self.logic_level += 1

        return self

    def end_logic(self):

        if self.logic_level < 1:
            raise RuntimeError('Tried to close a logic group without one open')

        if self._sql[-1] == '(':
            raise RuntimeError('Empty logic group')

        self._sql += ')'

        self.logic_level -= 1
        return self

    def where(self, col_name, operator, col_value, logic='AND', func=None, parent_query=None):

        self._check_col(col_name)

        if operator in Query.IS_OPERATORS:
            if col_value not in Query.IS_VALUES:
                msg = 'Values for "' + operator + '" operator must be one of ' + str(Query.IS_VALUES)
                msg += ', unknown value: ' + col_value
                raise ValueError(msg)
        elif operator not in Query.OPERATORS:
            raise ValueError('Unsupported operator: ' + operator)

        if logic and logic not in Query.LOGICAL:
            raise ValueError('Unsupported logic: ' + logic)

        if func and func not in Query.FUNCTIONS:
            raise ValueError('Unsupported function: ' + func)

        if ' WHERE ' in self._sql:
            if self._sql[-1] != '(':
                self._sql += ' '
                if logic:
                    self._sql += logic + ' '
        else:
            self._sql += ' WHERE '

        alias = self.alias and (self.alias + '"."') or ''
        column = f'"{alias}{col_name}"'

        if func:
            column = f'{func}({column})'

        # support "is null"
        # FUTURE: is there a more elegant way to support this?
        if operator in Query.IS_OPERATORS:
            self._sql += f'{column} {operator} {col_value}'
        elif parent_query:
            # this checks that the col value is a part of the parent, and applies an alias
            parent_query._check_col(col_value) # NOQA: SLF001

            if not parent_query.alias:
                raise RuntimeError('Alias is required when using a parent query column as a value')

            if self.alias == parent_query.alias:
                raise RuntimeError('Parent queries and sub queries must not have the same alias')

            parent_col_value = f'"{parent_query.alias}"."{col_value}"'

            self._sql += f'{column} {operator} {parent_col_value}'
        else:
            position = len(self.args) + 1

            # FUTURE: handle tuples? handle other logic matching conditions?
            pos = f'ANY(${position})' if isinstance(col_value, list) else f'${position}'

            self._sql += f'{column} {operator} {pos}'

            self.args.append(col_value)

        return self

    def add_logic(self, logic):
        if logic not in Query.LOGICAL:
            raise ValueError('Unsupported logic: ' + logic)

        self._sql += ' ' + logic

        return self

    def exists(self, subquery):

        if not isinstance(subquery, Query):
            raise TypeError('Subquery must be an instance of `Query`')

        sql = subquery.sql
        if subquery.args:
            # NOTE that because of the intermediary replacement mechanism below we have to limit the number of args
            # this is determined by ascii z (122) - A (65) = 57
            # FUTURE: this seems like a very high limit we won't hit, but probably a better way to do this anyway?
            # NOTE: actually this falls apart with double digits because replace('$1') will catch '$10'
            # need some kind of solution for that (running the loop backwards?)
            if len(subquery.args) > MAX_SUBQUERY_ARGS:
                raise RuntimeError('Too many args in subquery: ' + str(len(subquery.args)))

            # to avoid conflicts where we do something like replace $1 with $2 and then accidentally replace
            # the replaced $2 with something else instead of the actual placeholder $2 later
            # we replace everything with a letter first, and then go back and replace with the actual number
            for i, _arg in enumerate(subquery.args):
                sql = sql.replace(f'${i + 1}', f'${chr(i + 65)}')

            position = len(self.args) + 1
            for i, arg in enumerate(subquery.args):
                sql = sql.replace(f'${chr(i + 65)}', f'${position}')
                self.args.append(arg)
                position += 1

        # WARNING - the subquery can be anything right now - do not expose to end users like this!
        self._sql += ' EXISTS (' + sql + ')'

        return self

    def order_by(self, col_name, direction='ASC'):

        self._check_col(col_name)

        if direction not in Query.DIRECTIONS:
            raise ValueError('Unsupported direction: ' + direction)

        if ' ORDER BY ' in self.order_by_sql:
            self.order_by_sql += ','
        else:
            self.order_by_sql += ' ORDER BY'

        self.order_by_sql += f' "{col_name}" {direction}'

        return self

    # FUTURE: do we need to support this?
    # def group_by(self, col_name):

    def limit(self, n: int):

        if not isinstance(n, int):
            raise TypeError('Limit must be an integer: ' + str(n))

        if n < 1:
            raise ValueError('Limit must be greater than zero: ' + str(n))

        if n > MAX_LIMIT:
            raise ValueError('Limit must be 10000 or less: ' + str(n))

        if ' LIMIT ' in self.limit_sql:
            raise RuntimeError('Multiple calls to limit on the same query are not allowed')

        self.limit_sql += f' LIMIT {n}'

        return self

    def offset(self, n: int):

        if not isinstance(n, int):
            raise TypeError('Offset must be an integer: ' + str(n))

        if n < 0:
            raise ValueError('Offset must not be negative: ' + str(n))

        if ' OFFSET ' in self.offset_sql:
            raise RuntimeError('Multiple calls to offset on the same query are not allowed')

        self.offset_sql += f' OFFSET {n}'

        return self

    async def all(self):

        if self.logic_level > 0:
            raise RuntimeError('Tried to query without closing all logic groups')

        return [self.model_class.convert(row) for row in await self.conn.fetch(self.sql, *self.args)]

    async def count(self):

        if self.logic_level > 0:
            raise RuntimeError('Tried to query without closing all logic groups')

        # NOTE that we purposefully use the _sql here that doesn't have endings applied
        # because those can mess with the count
        # NOTE the 1 is important here so we don't replace the select on subqueries if they exist
        sql = self._sql.replace('SELECT * FROM', 'SELECT COUNT(*) FROM', 1)

        return await self.conn.fetchval(sql, *self.args)

    async def first(self):

        if self.logic_level > 0:
            raise RuntimeError('Tried to query without closing all logic groups')

        return self.model_class.convert(await self.conn.fetchrow(self.sql, *self.args))


class Model:

    _fields = None
    _fields_class = None

    class Meta:
        # optional defaults:
        schema = 'public'

        # required on child classes (no default):
        # table = ''

        # constraints = {}

    # docs: https://magicstack.github.io/asyncpg/current/api/index.html
    # examples: https://github.com/jordic/fastapi_asyncpg/blob/master/fastapi_asyncpg/sql.py

    # FUTURE: can we generalize checking these values and turning them into properties
    # so we don't have to do each individually?

    # also NOTE that caching these by setting the values on the class after first access can cause big problems
    # we have cls.schema_table = schema_table at the end of that and it cached the BASE class version for all children
    # caching stuff like that will have to take the cls.__name__ into account
    @classmethod
    @property
    def meta_table(cls):
        return hasattr(cls.Meta, 'table') and cls.Meta.table or cls.__name__.lower()

    @classmethod
    @property
    def meta_schema(cls):
        return hasattr(cls.Meta, 'schema') and cls.Meta.schema or Model.Meta.schema

    @classmethod
    @property
    def schema_table(cls):
        # NOTE: if schema is quoted together with the table name then postgres assumes it's in the public schema
        # e.g. "public.user" gets converted to "public.public.user" which doesn't exist
        # could still put schema in quotes separately if we're concerned ("schema"."table")
        # but they're all system defined so it shouldn't be an issue
        return f'{cls.meta_schema}."{cls.meta_table}"'

    @classmethod
    @property
    def fields(cls):
        # NOTE: this guard is needed to avoid infinite recursion caused by the getmembers call
        # and the class name keeps earlier calls to base classes from overriding child classes
        if cls._fields is None or cls._fields_class != cls.__name__:
            cls._fields_class = cls.__name__
            cls._fields = {}

            attributes = inspect.getmembers(cls, lambda a: not(inspect.isroutine(a)))
            attrs = [a for a in attributes if not a[0].startswith('_')]
            for name, field in attrs:
                if isinstance(field, Field):
                    # NOTE: the strip here allows for correcting conflicts between built in methods and field names
                    # e.g. there's a `create` method so we call a field `create_` and it works because of this
                    cls._fields[name.strip('_')] = field

        return cls._fields

    @classmethod
    def convert(cls, item):
        if not item:
            return None

        # NOTE: when this is called the item is still a Record type from asyncpg
        return dict(item)

    # NOTE that these all return dicts rather than objects, and that's ok, probably preferable
    @classmethod
    async def get(cls, conn, record_id):

        return cls.convert(await conn.fetchrow(f'SELECT * FROM {cls.schema_table} WHERE id = $1', record_id)) # NOQA: S608

    @classmethod
    def select(cls, conn, columns=None, alias=None):
        return Query(conn, cls, columns=columns, alias=alias)

    @classmethod
    async def create(cls, conn, **kwargs):
        fields = cls.fields
        names = []
        values = []

        # FUTURE: we could loop through the fields instead
        # and then if we wanted to we could have a python only default (i.e. not controlled by the database)
        for name, value in kwargs.items():
            if name in fields:
                if name == 'id':
                    raise KeyError(f'Primary key field {name} is auto generated, do not specify')

                # NOTE: letting the database exclusively worry about values, nulls, uniques, etc. for now
                names.append('"' + name + '"')
                values.append(value)
            else:
                raise KeyError(f'Unknown field {name}')

        sql = f'INSERT INTO {cls.schema_table} '

        # we have to check for this having something in it because it's possible all values are defaults
        if names:
            # NOTE that we use the python formatting to create the $1, $2, $3, etc. places for sql
            # but then pass the actual values through the function rather than trying to format them
            places = ', '.join([f'${i}' for i in range(1, len(values) + 1)])
            sql += f'({", ".join(names)}) VALUES ({places}) '
        else:
            sql += 'DEFAULT VALUES '

        # NOTE: we want to be sure to use "RETURNING *" here so we can send back data exactly as the db has it
        sql += 'RETURNING *'

        return cls.convert(await conn.fetchrow(sql, *values))

    # FUTURE: support bulk insert
    # NOTE: for bulk insert we can use executemany, but that doesn't return ids
    # see https://stackoverflow.com/questions/43739123/best-way-to-insert-multiple-rows-with-asyncpg
    # which includes an answer on how to do that if we need it

    # async def insert(conn, table, values):
    #     qs = "insert into {table} ({columns}) values ({values}) returning *".format(
    #         table=table,
    #         values=",".join([f"${p + 1}" for p in range(len(values.values()))]),
    #         columns=",".join(list(values.keys())),
    #     )
    #     return await conn.fetchrow(qs, *list(values.values()))

    # FUTURE: support bulk update
    @classmethod
    async def update(cls, conn, record_id, **kwargs):
        # FUTURE: this only works for a single record, create a batch version for multiple at once
        fields = cls.fields
        names = []
        values = []

        # FUTURE: we could loop through the fields instead
        # and then if we wanted to we could have python only logic for updates (i.e. not controlled by the database)
        for name, value in kwargs.items():
            if name in fields:
                if name == 'id':
                    raise KeyError(f'Primary key field {name} is auto generated, do not specify')

                # NOTE: letting the database exclusively worry about values, nulls, uniques, etc. for now
                names.append('"' + name + '"')
                values.append(value)
            else:
                raise KeyError(f'Unknown field: {name}')

        if not names:
            raise ValueError('No fields to update')

        # NOTE: we want to be sure to use "RETURNING *" here so we can send back data exactly as the db has it
        # also note that we use the python formatting to create the $1, $2, $3, etc. places for sql
        # but then pass the actual values through the function rather than trying to format them
        columns = ', '.join([f'{names[i]}=${i + 1}' for i in range(len(values))])

        sql = f'UPDATE {cls.schema_table} SET {columns} WHERE id=${len(values) + 1} RETURNING *' # NOQA: S608

        values.append(record_id)
        return cls.convert(await conn.fetchrow(sql, *values))

    # FUTURE: the update and create methods are very similar - could we combine them somehow?

    @classmethod
    async def delete(cls, conn, record_id):
        # this returns the text "DELETE N" where N is the amount of things deleted
        response = await conn.execute(f'DELETE FROM {cls.schema_table} WHERE id = $1', record_id) # NOQA: S608
        _delete, amount = response.split(' ', 1)
        return int(amount)

    @classmethod
    async def delete_where(cls, conn, col_name, operator, col_value, and_name=None, and_operator=None, and_value=None):

        fields = cls.fields

        if col_name not in fields:
            raise KeyError(f'Unknown field: {col_name}')

        if operator in Query.IS_OPERATORS:
            if col_value not in Query.IS_VALUES:
                msg = 'Values for "' + operator + '" operator must be one of ' + str(Query.IS_VALUES)
                msg += ', unknown value: ' + col_value
                raise ValueError(msg)
        elif operator not in Query.OPERATORS:
            raise ValueError('Unsupported operator: ' + operator)

        if and_operator:
            if and_operator in Query.IS_OPERATORS:
                if and_value not in Query.IS_VALUES:
                    msg = 'Values for "' + and_operator + '" operator must be one of ' + str(Query.IS_VALUES)
                    msg += ', unknown value: ' + and_value
                    raise ValueError(msg)
            elif and_operator not in Query.OPERATORS:
                raise ValueError('Unsupported operator: ' + and_operator)

        # this returns the text "DELETE N" where N is the amount of things deleted
        sql = f'DELETE FROM {cls.schema_table} WHERE "{col_name}" {operator} ' # NOQA: S608

        if isinstance(col_value, list):
            sql += 'ANY($1)'
        else:
            sql += '$1'

        args = [col_value]

        # support a single and clause to allow for exclusions
        # FUTURE: this is already pretty complicated, any more and we need to refactor
        # could try to combine with query, but a lot of that stuff (order by, offset, etc.) doesn't apply here
        if and_name and and_operator and and_value:
            sql += f' AND "{and_name}" {and_operator} $2'
            args.append(and_value)

        response = await conn.execute(sql, *args)
        _delete, amount = response.split(' ', 1)
        return int(amount)

    @classmethod
    def _generateColumns(cls, fields):
        columns = []
        constraints = []
        after_constraints = []
        for col_name, field in fields.items():
            column, constraint = field.create(cls.schema_table, col_name)

            columns.append(column)

            if constraint:
                # some constraints like foreign keys can be included during table creation
                # but others can only be done later because they rely on executing a separate sql command
                # for doing things like creating functions and triggers
                if constraint.startswith('CONSTRAINT'):
                    constraints.append(constraint)
                else:
                    # because after constraints are executed all together they need to be separate commands
                    # so we enforce that here
                    if not constraint.endswith(';'):
                        constraint += ';'

                    after_constraints.append(constraint)

        return columns, constraints, after_constraints

    @classmethod
    async def createTable(cls, conn):

        fields = cls.fields

        if not fields:
            raise ValueError('No fields found on model')

        if 'id' not in fields:
            raise KeyError('"id" column must be explicitly defined')

        columns, constraints, after_constraints = cls._generateColumns(fields)

        # also include constraints from the table itself here
        # NOTE that these are assumed to all be check constraints only
        if hasattr(cls.Meta, 'constraints'):
            for name, constraint in cls.Meta.constraints.items():
                constraints.append('CONSTRAINT ' + name + ' ' + constraint)

        # add constraints to the end of the list
        columns.extend(constraints)

        sql = f'CREATE TABLE {cls.schema_table} ({", ".join(columns)})'

        await conn.execute(sql)

        if after_constraints:
            # each after constraint is expected to be a full statement and thus end in a semicolon - see check above
            await conn.execute(' '.join(after_constraints))

        if hasattr(cls.Meta, 'unique_indexes'):
            for name, index in cls.Meta.unique_indexes.items():
                await conn.execute(f'CREATE UNIQUE INDEX "{name}" ON {cls.schema_table} ({index})')

        if hasattr(cls.Meta, 'indexes'):
            for name, index in cls.Meta.indexes.items():
                await conn.execute(f'CREATE INDEX "{name}" ON {cls.schema_table} USING {index}')

    @classmethod
    async def dropTable(cls, conn):
        sql = f'DROP TABLE {cls.schema_table}'
        await conn.execute(sql)

    @classmethod
    async def addColumns(cls, conn, fields):

        if not fields:
            raise ValueError('No fields found on model')

        if 'id' in fields:
            raise KeyError('"id" column can not be added after table creation')

        columns, constraints, after_constraints = cls._generateColumns(fields)

        # alter table needs additional verbage vs create table
        # columns need "add column"
        columns = ['ADD COLUMN ' + column for column in columns]

        # and constraints need "add"
        constraints = ['ADD ' + constraint for constraint in constraints]

        # add constraints to the end of the list
        columns.extend(constraints)

        sql = f'ALTER TABLE {cls.schema_table} {", ".join(columns)}'

        await conn.execute(sql)

        if after_constraints:
            # each after constraint is expected to be a full statement and thus end in a semicolon - see check above
            await conn.execute(' '.join(after_constraints))
