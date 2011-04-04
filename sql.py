import copy
import datetime
from types import NoneType
import operator
import sqlite3

class Db(object):
    _settings = {
        # 'engine': 'sqlite',
        # 'name': 'somewhere',
    }

    def __init__(self, **kwargs):
        self._settings['engine'] = kwargs['engine']
        self._settings['name'] = kwargs['name']
        if self._settings['engine'] == 'sqlite':
            self.__connection = sqlite3.connect(self._settings['name'])
        else:
            raise Exception("Not Implemented")

    def _execute(self, query):
        if self._settings['engine']:
            return self.__connection.execute(query)
        else:
            raise Exception("Not Implemented")

    def __getattr__(self, name):
        return Table(name)

UPDATE = 'UPDATE'
SELECT = 'SELECT'
DELETE = 'DELETE'

BINARY_OPERATORS = ('=', '!=', '<', '<=', '>', '>=', 'IN')

class Expr(object):
    negative = False
    children = [] # sub-expressions or literals
    operator = None # what connects them

    def __new__(cls, *args, **kwargs):
        """
        creates new Expr object or returns existing,
        if called with single parameter of type Expr.
        """
        if len(args) == 1 and isinstance(args[0], Expr):
            return args[0]
        # default object
        return object.__new__(cls)

    def __init__(self, *args, **kwargs):
        """ Initializes the Expression, accepting parameters as its children
        simple types are treated as Literals for future sql representation
        """
        # except the case where just one expression was passed.
        # this situation is handled in __new__
        if args and (len(args) != 1 or not isinstance(args[0], Expr)):
            children = []
            for child in args:
                if not isinstance(child, (Expr, Param, Field)):
                    child = Literal(child)
                children.append(child)
            self.children = children
            self.operator = kwargs.get('operator')

    def join(self, other, operator):
        """
        joins this Expression with other one.
        if current is leaf object, then it is moved downwards as first child
        and other one is added as second child

        also that is performed when they are of different type etc.
        when possible through other is added to children list
        """
        if not isinstance(other, Expr):
            other = Expr(other)
        # if current is not leaf and merge seems possible
        if (self.operator == operator and not self.negative
                # other is either multicond of the same operation type,
                # non negated
                and (other.operator == operator and not other.negative
                    # or is a leaf:
                    or not other.is_multi())
                ):
                # if multicond, we merge his kids with ours
                if other.is_multi():
                    self.children.extend(other.children)
                # if a leaf, we add it to list of our children
                else:
                    self.children.append(other)
                return self
        else:
            # shallowcopy ourselves into a new object, to be moved downside
            obj = copy.copy(self)
            # that is brand new me, with new operator and kids
            self.operator = operator
            self.children = [obj, other]
            return self

    def is_multi(self):
        """ returns True when this Expr contains other Exprs """
        return [c for c in self.children if isinstance(c, Expr)]

    def __or__(self, other):
        return self.join(other, "OR")
    def __and__(self, other):
        return self.join(other, "AND")
    def __invert__(self):
        self.negative = not self.negative
        return self
    def __eq__(self, other):
        return self.join(other, '=')
    def __ne__(self, other):
        return self.join(other, '!=')
    def __lt__(self, other):
        return self.join(other, '<')
    def __le__(self, other):
        return self.join(other, '<=')
    def __gt__(self, other):
        return self.join(other, '>')
    def __ge__(self, other):
        return self.join(other, '>=')
    def __add__(self, other):
        return self.join(other, '+')
    def __sub__(self, other):
        return self.join(other, '-')
    def __mul__(self, other):
        return self.join(other, '*')
    def __div__(self, other):
        return self.join(other, '/')
    def _in_(self, other):
        return self.join(other, 'IN')

    def __repr__(self):
        if self.is_multi(): # nested condition
            return ("<Expr: %s>" % self.operator)
        else:
            return "<Expr: %s>" % (" %s " % self.operator).join([
                repr(child) for child in self.children])

    def sql(self, **kwargs):
        def sqlize(obj, **kwargs):
            if callable(getattr(obj, 'sql', None)):
                return obj.sql(**kwargs)
            else:
                return str(obj)
        if self.operator is None:
            assert len(self.children) == 1, "No operator for multichild Expr"
            res = sqlize(self.children[0], **kwargs)
            if self.negative:
                res = "NOT(%s)" % res
            return res
        else:
            # special case of IS NONE
            if self.operator in ('=', '!=') and len(self.children) == 2 \
                and sqlize(self.children[1], **kwargs) == 'NULL':
                operator = 'IS' if self.operator == '=' else 'IS NOT'
            else:
                operator = self.operator
            return "%(not)s(%(expr)s)" % {
                'not': 'NOT' if self.negative else '',
                'expr':(" %s " % operator).join(
                            [sqlize(c, **kwargs) for c in self.children])
                }

    __str__ = sql


class Overloaded(object):
    def __eq__(self, other):
        return Expr(self, other, operator='=')
    def __ne__(self, other):
        return Expr(self, other, operator='!=')
    def __lt__(self, other):
        return Expr(self, other, operator='<')
    def __le__(self, other):
        return Expr(self, other, operator='<=')
    def __gt__(self, other):
        return Expr(self, other, operator='>')
    def __ge__(self, other):
        return Expr(self, other, operator='>=')
    def __add__(self, other):
        return Expr(self, other, operator='+')
    def __sub__(self, other):
        return Expr(self, other, operator='-')
    def __mul__(self, other):
        return Expr(self, other, operator='*')
    def __div__(self, other):
        return Expr(self, other, operator='/')
    def _in_(self, other):
        return Expr(self, other, operator='IN')


class Literal(Overloaded):
    """
    something passed to the query that needs to be transformed according to
    database conventions
    """
    # good for testing, but in general case should be unnecessary
    default_db = None

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "<Literal:%s>" % self.sql()

    def bool_converter(value, db):
        if db in ('postgres', 'rdbhost'):
            return "'t'" if value else "'f'"
        else:
            return '1' if value else '0'

    def string_converter(value, db):
        replaces = [
            ("'", "''"),
            ('\\', '\\\\'),
            ('\000', '\\0'),
            ('\b', '\\b'),
            ('\n', '\\n'),
            ('\r', '\\r'),
            ('\t', '\\t'),
        ]
        if db in ('mysql', 'postgres', 'rdbhost'):
            for orig, repl in replaces:
                value = value.replace(orig, repl)
        elif db in ('sqlite', 'firebird', 'sybase', 'maxdb', 'mssql'):
            value = value.replace("'", "''")
        else:
            raise Exception("Database %s unknown" % db)
        return "'%s'" % value

    converters = {
        int: lambda value, db: str(value),
        long: lambda value, db: str(value),
        float: lambda value, db: str(value),
        bool: bool_converter,
        str: string_converter,
        unicode: string_converter,
        datetime.date:
            lambda value, db: "'%04d-%02d-%02d'" % (
                value.year, value.month, value.day),
        datetime.time:
            lambda value, db: "'%02d:%02d:%02d'" % (
                value.hour, value.minute, value.second),
        datetime.datetime:
            lambda value, db: "'%04d-%02d-%02d %02d:%02d:%02d'" % (
                value.year, value.month, value.day,
                value.hour, value.minute, value.second),
        datetime.timedelta:
            lambda value, db: "INTERVAL '%d days %d seconds'" % (
                value.days, value.seconds),
        NoneType: lambda value, db: "NULL",
    }

    def sql(self, **kwargs):
        db = kwargs.get('db', self.default_db)
        if not db:
            raise Exception("Undefined db")
        if type(self.value) in self.converters:
            return self.converters[type(self.value)](self.value, db)
        else:
            try:
                # may be it is iterable?
                iter(self.value)
                return "(%s)" % ", ".join(
                    [Literal(v).sql(db=db) for v in self.value])
            except TypeError:
                raise Exception("No converter for %s" % type(self.value))

class Param(Overloaded):
    """ A parameter that can be passed to Expr and thus to SqlBuilder
    """
    name = ''

    def __init__(self, name):
        self.name = name

    def sql(self, **kwargs):
        if 'params' not in kwargs or self.name not in kwargs['params']:
            raise Exception('parameter "%s" not found' % self.name)
        return Literal(kwargs['params'][self.name]).sql(**kwargs)

    def __repr__(self):
        return "<Param:%s>" % self.name


class Field(Overloaded):
    def __init__(self, table, name):
        self.table = table
        self.name = name

    def __str__(self):
        return "%s.%s" % (str(self.table), self.name)

    def __repr__(self):
        return "<Field:%s>" % str(self)


class Table(object):
    def __init__(self, name):
        # to avoid confusion with pretty common field 'name'
        self.__name = name

    def __repr__(self):
        return "<Table:%s>" % self.__name

    def __str__(self):
        return self.__name

    def __getattr__(self, name):
        return Field(self, name)


class SqlBuilder(object):
    """ the query builder """

    def __init__(self):
        self.query_type = None
        self.select_fields, self.from_tables, = None, None
        self.set_fields = None
        self.where_conds, self.having_conds = None, None
        self.joins = []
        self.limit = None
        self.params = []

    def Select(self, *args, **kwargs):
        assert self.query_type is None, \
            ".Select() can not be called once query type has been set"
        self.query_type = SELECT
        # TODO: field aliases, tables in the list, to indicate table.*
        self.select_fields = args
        return self

    def Update(self, update_table):
        assert self.query_type is None, \
            ".Update() can not be called once query type has been set"
        assert isinstance(update_table, Table), "Update accepts only tables"
        self.query_type = UPDATE
        self.update_table = update_table
        return self

    def Set(self, *args, **kwargs):
        assert self.query_type == UPDATE, \
            ".Set() is only available for Update() queries"
        self.set_fields = [(field, Expr(expr)) for (field, expr)
                in (list(args) + list(kwargs.items()))]
        return self

    def Delete(self):
        assert self.query_type is None, \
            ".Delete() can not be called once query type has been set"
        self.query_type = DELETE
        return self

    def From(self, *args, **kwargs):
        assert self.query_type in (SELECT, DELETE), \
            "From() is available only for Select() and Update() queries."
        # TODO: tables aliases
        self.from_tables = args
        return self

    def Where(self, *args):
        self.where_conds = reduce(operator.and_, args)
        return self

    def And(self, *args):
        assert self.where_conds or self.having_conds, \
            ".And() can be called only after .Where() or .Having()"
        self.where_conds &= reduce(operator.and_, args)
        return self

    def Or(self, *args):
        assert self.where_conds or self.having_conds, \
            ".Or() can be called only after .Where() or .Having()"
        self.where_conds |= reduce(operator.or_, args)
        return self

    def Join(self, table, join_type, *args):
        assert isinstance(table, Table), "Join only accepts tables"
        self.joins.append({
            'table': table,
            'conds': reduce(operator.and_, args) if args else None,
            'type' : join_type,
            })
        return self
    def InnerJoin(self, table, *args):
        return self.Join(table, "INNER", *args)
    def LeftJoin(self, table, *args):
        return self.Join(table, "LEFT OUTER", *args)
    def RightJoin(self, table, *args):
        return self.Join(table, "RIGHT OUTER", *args)
    def OuterJoin(self, table, *args):
        return self.Join(table, "FULL OUTER", *args)

    def GroupBy(self, *args):
        # TODO
        return self

    def Having(self, *args):
        # TODO
        return self

    def OrderBy(self, *args):
        # TODO
        return self

    def Limit(self, num_rows):
        self.limit = num_rows
        return self

    def sql(self, db=None):
        """ Construct sql to be executed
        db parameter indicates type of database engine
        """
        if self.query_type == UPDATE:
            assert self.set_fields, "No field setting rules issued, use Set()"
            res = "UPDATE %s SET %s" % (
                self.update_table,
                ", ".join(["%s = %s " %
                    (str(field), expr.sql(params=self.params, db=db))
                                    for (field, expr) in self.set_fields ]),
                )
        elif self.query_type == SELECT:
                res = "SELECT %s" % (", ".join(
                    [str(field) for field in self.select_fields]) or "*")
        elif self.query_type == DELETE:
                res = "DELETE"
        else:
            raise Exception("Unknown query type")
        if self.query_type in (SELECT, DELETE):
            assert self.from_tables, "From() clause is required."
            res += " FROM %s" % ", ".join(
                [str(table) for table in self.from_tables])
        if self.joins:
            for j in self.joins:
                res += " %(type)s JOIN %(table)s " % j
                if j['conds']:
                    res += "ON %s" % j['conds'].sql(params=self.params, db=db)

        if self.where_conds:
            res +=" WHERE %s" % self.where_conds.sql(params=self.params, db=db)
        if self.limit:
            res +="LIMIT %s" % self.limit
        return res

    def FetchFrom(self, db):
        if self.query_type == SELECT:
            return ResultIterator(self.select_fields,
                db._execute(self.sql(db=db._settings['engine']))
                )

class ResultIterator(object):
    def __init__(self, fields, cursor):
        self.short_fields = [field.name.lower() for field in fields]
        self.long_fields = [("%s__%s" % (str(field.table), field.name)).lower()
                 for field in fields]
        self.cursor = cursor

    def next(self):
        return RowWrapper(self.cursor.next(),
            self.short_fields, self.long_fields)

    def __iter__(self):
        return self

class RowWrapper(object):
    def __init__(self, values, short_fields, long_fields):
        self.values = values
        self.short_fields = short_fields
        self.long_fields = long_fields

    def __getattr__(self, attr):
        attr = attr.lower()
        if not self.values:
            return
        if attr in self.short_fields:
            return self.values[self.short_fields.index(attr)]
        if attr in self.long_fields:
            return self.values[self.long_fields.index(attr)]

    def __repr__(self):
        return repr(self.values)
    def __str__(self):
        return str(self.values)
    def __unicode__(self):
        return unicode(self.values)
    def __iter__(self):
        return iter(self.values)
    def __getitem__(self, key):
        return self.values[key]
    def __len__(self):
        return len(self.values)
