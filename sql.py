import copy
import datetime
from types import NoneType
import operator
from collections import Iterable
import sqlite3

class Db(object):
    """
    Provides Db connection. Returns Tables as its properties.
    """
    _settings = {}

    def __init__(self, **kwargs):
        self._settings['engine'] = kwargs['engine']
        self._settings['name'] = kwargs['name']
        # Only sqlite for now
        if self._settings['engine'] == 'sqlite':
            self.__connection = sqlite3.connect(self._settings['name'])
        else:
            raise Exception("DB Backend not Implemented")

    def _execute(self, query):
        """Execute given SQL, return cursor."""
        if self._settings['engine']:
            return self.__connection.execute(query)
        else:
            raise Exception("DB Backend not Implemented")

    def __getattr__(self, name):
        """Return Table with given name."""
        return Table(name)

class Table(object):
    """
    Used in constructing SQL and also returns Fields as its properties.
    Is not checked for presence in database.
    """
    def __init__(self, name):
        # to avoid confusion with pretty common field 'name'
        self.__name = name

    def __repr__(self):
        return "<Table:%s>" % self.__name

    def __str__(self):
        return self.__name

    def __getattr__(self, name):
        """Return Fields with given name."""
        return Field(self, name)

UPDATE = 'UPDATE'
SELECT = 'SELECT'
DELETE = 'DELETE'

BINARY_OPS = ('=', '!=', '<', '<=', '>', '>=', 'IN')

Max = lambda obj: Expr(obj).apply_func("MAX")
Min = lambda obj: Expr(obj).apply_func("MIN")
Avg = lambda obj: Expr(obj).apply_func("AVG")
Sum = lambda obj: Expr(obj).apply_func("AVG")
First = lambda obj: Expr(obj).apply_func("FIRST")
Last = lambda obj: Expr(obj).apply_func("LAST")

def Count(obj=None):
    expr = Expr(obj) if obj else Expr()
    return expr.apply_func("COUNT")

class Expr(object):
    """
    Represents arithmetic and logical expressions in SQL.
    Supports SQL functions. Also is returned as a result of arithmetic or
    logical operations on Field, Literal, Param and Alias.

    Usage: Expr(..) + Expr(..),
        Expr(..) > Literal(..),
        (Expr(..) + Expr(..) + Expr(..) ) * Param(..)
    Logical operations are performed using binary logic operators:
        (Expr(..) | ~Expr(..) | Expr(..) ) & Param(..)
        IN (...) is expressed as Expr(..)._in_(List(..))

    Be aware that the order of operations is not working, use brackets.

    When Expr has to do operation with object of a simple type like <int>
    or <str>, it converts given object to Literal.

    Expr.sql(..) is used to get representation of given Expr in given Db,
    with given parameter list.

    """
    def __new__(cls, *args, **kwargs):
        """
        Creat new Expr object or return existing,
        if called with single parameter of type Expr.
        """
        if len(args) == 1 and isinstance(args[0], Expr):
            return args[0]
        # default object
        obj = object.__new__(cls)
        obj.func = '' # there can be a function to be applied to the result
        obj.children = [] # sub-expressions or literals
        obj.operator = None # what connects them
        return obj

    def __init__(self, *args, **kwargs):
        """
        Initialize the Expression, accepting parameters as its children.
        Simple types are treated as Literals for future SQL representation.
        """
        # except the case where just one expression was passed.
        # this situation is handled in __new__
        if args and (len(args) != 1 or not isinstance(args[0], Expr)):
            children = []
            for child in args:
                if not isinstance(child, (Expr, Param, Field, Alias)):
                    child = Literal(child)
                children.append(child)
            self.children = children
            self.operator = kwargs.get('operator')

    def join(self, other, operator):
        """
        Joins this Expression with other one.

        If current is a leaf object, then it is moved downwards as first child
        and other one is added as second child

        Also that is performed when they are of different types,
        or has functions on them
        """
        if not isinstance(other, Expr):
            other = Expr(other)
        # if current is not leaf and merge seems possible
        if (operator not in BINARY_OPS and
                self.operator == operator and not self.func
                # other is either multicond of the same operation type,
                # no functional call neither on us nor on him
                and (other.operator == operator and not other.func
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
            self.func = ''
            self.operator = operator
            self.children = [obj, other]
            return self

    def is_multi(self):
        """Return True when this Expr contains other Exprs."""
        return [c for c in self.children if isinstance(c, Expr)]

    def apply_func(self, func):
        """
        Apply given SQL function to children.
        If this node already have a function, then it is shifted downward,
        and new one with needed function is added in its place
        """
        if not self.func:
            self.func = func
            return self
        else:
            obj = Expr()
            obj.children.append(self)
            obj.func = func
            return obj

    def __or__(self, other):
        return self.join(other, "OR")
    def __and__(self, other):
        return self.join(other, "AND")
    def __invert__(self):
        return self.apply_func("NOT")
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
        """Represent this Expr using short notation and children's repr."""
        if self.is_multi(): # nested condition
            return ("<Expr: %s>" % self.operator)
        else:
            return "<Expr: %s>" % (" %s " % self.operator).join([
                repr(child) for child in self.children])

    def sql(self, **kwargs):
        """
        Generate SQL for this query.
        Accepts anything and passes it down the rendering stack,
        let the children pick what they need.
        Namely, Param needs params dict, Literal needs db string (engine type)
        """
        def sqlize(obj, **kwargs):
            """
            Call specialized .sql() on the object supporting it.
            Call str() otherwise.
            """
            if callable(getattr(obj, 'sql', None)):
                return obj.sql(**kwargs)
            else:
                return str(obj)

        # That indicates that this is a leaf, or even special leaf *
        if self.operator is None:
            assert len(self.children) <= 1 or self.func, \
               "Only function calls on * or Literal can go without an operator"
            if self.children:
                # that's the only child
                res = sqlize(self.children[0], **kwargs)
            else:
                # childless node. If we got there, assume user wants a star.
                res = "*"
            if self.func:
                res = "%s(%s)" % (self.func, res)
            return res
        else:
            # special handling of IS NULL and IS NOT NULL cases
            if self.operator in ('=', '!=') and len(self.children) == 2 \
                and sqlize(self.children[1], **kwargs) == 'NULL':
                operator = 'IS' if self.operator == '=' else 'IS NOT'
            else:
                operator = self.operator
            return "%(func)s(%(expr)s)" % {
                'func': self.func,
                'expr':(" %s " % operator).join(
                            [sqlize(c, **kwargs) for c in self.children])
                }
    # str() is not really used in SqlBuilder, but it is handy for testing
    __str__ = sql


class Overloaded(object):
    """
    Replaces logical and arithmetic operations with Expr's implementation.
    Adds ._in_() function for issuing IN (...) condition
    """
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
    Represents object of a simple type passed to the query.
    .sql() does the transformation according to database conventions
    """
    # good for testing, but in general case should be unnecessary
    default_db = None

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "<Literal:%s>" % self.sql()

    def bool_converter(value, db):
        """Convert boolean value for use in database."""
        if db in ('postgres', 'rdbhost'):
            return "'t'" if value else "'f'"
        else:
            return '1' if value else '0'

    def string_converter(value, db):
        """Convert string value for use in database."""
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
        """Convert self.value to its string representation,
        fit for passing to database.
        """
        db = kwargs.get('db', self.default_db)
        if not db:
            raise Exception("Undefined db")
        # find one in our list
        if type(self.value) in self.converters:
            return self.converters[type(self.value)](self.value, db)
        else:
            # may be it is iterable?
            if isinstance(self.value, Iterable):
                return "(%s)" % ", ".join(
                    [Literal(v).sql(db=db) for v in self.value])
            else:
                raise Exception("No converter for %s" % type(self.value))

class Param(Overloaded):
    """
     A parameter that can be passed to Expr and thus to SqlBuilder
    """
    def __init__(self, name):
        self.name = name

    def sql(self, **kwargs):
        """
        Substitute the parameter with value in passed params,
        then get SQL representation with Literal's help.
        """
        if 'params' not in kwargs or self.name not in kwargs['params']:
            raise Exception('parameter "%s" not found' % self.name)
        return Literal(kwargs['params'][self.name]).sql(**kwargs)

    def __repr__(self):
        return "<Param:%s>" % self.name

class Alias(Overloaded):
    """
    Field alias that can be used in expressions.
    Does not get escaped at all.
    """
    def __init__(self, name):
        self.name = name

    def sql(self, **kwargs):
        """
        Get SQL represenation of self
        Since the Alias is meant to be passed as is, just return self.name
        """
        return self.name

    def __repr__(self):
        return "<Alias:%s>" % self.name


class Field(Overloaded):
    """
    Represents table field. Used in SQL expressions.
    """
    def __init__(self, table, name):
        self.table = table
        self.name = name

    def __str__(self):
        return "%s.%s" % (str(self.table), self.name)

    def __repr__(self):
        return "<Field:%s>" % str(self)


class SqlBuilder(object):
    """
    Builds SQL query.

    Supports SELECT, UPDATE and DELETE queries. Syntax:

    Select(field or tuple, ..).From(table or tuple, ..).Where(Expr, ..).
        Having(Expr, ..).GroupBy(Expr).Limit(n..)

    Delete().From(table or tuple, ..).Where(Expr, ..).Limit(n..)

    Update(table).Set((Field, Expr),..).Where(Expr, ..)

    NOTE: Presense of all fields and table.fields currently is not enforced, so
    if you pass db.x.y when table x does not exist and is not present in any
    FROM clauses the query will still be executed.

    Query is evaluated when you issue .FetchRows(db)
        where db is open database connection of type Db()
    """

    def __init__(self):
        """Initialize the sql with empty values, to make checking simpler."""
        self.query_type = None
        self.select_fields = []
        self.from_tables = []
        self.where_conds = []
        self.having_conds = []
        self.group_fields = []
        self.set_fields = []
        self.joins = []
        self.limit = None
        self.params = []

    def Select(self, *args):
        """
        Fill fields which are to be selected.
        Each parameter is either a field or a tuple of (Field, alias).
        Latter ones will be represented in SQL as "field as alias"
        Alias is a string, it is not escaped because is meant to be used as-is.
        """
        assert self.query_type is None, \
            ".Select() can not be called once query type has been set"
        self.query_type = SELECT
        for arg in args:
            if isinstance(arg, (Field, Table, Expr)):
                self.select_fields.append(arg)
            # we accept tuples and lists here, which are surely Iterable
            # if somebody passes Iterable without __getitem__ he will get excp
            elif isinstance(arg, Iterable) \
                and isinstance(arg[0], (Field, Expr)):
                self.select_fields.append(arg[:2])
        return self

    def Update(self, update_table):
        """Fill in the name of the table to be updated."""
        assert self.query_type is None, \
            ".Update() can not be called once query type has been set"
        assert isinstance(update_table, Table), "Update accepts only tables"
        self.query_type = UPDATE
        self.update_table = update_table
        return self

    def Set(self, *args, **kwargs):
        """
        Fill in the list of fields to be updated and their respective Exprs.
        Parameters:
            Ordered : tuples of (Field, Expr).
            Keywords: alias=Expr, where alias is a string.
        """
        assert self.query_type == UPDATE, \
            ".Set() is only available for Update() queries"
        self.set_fields = [(field, Expr(expr)) for (field, expr)
                in (list(args) + list(kwargs.items()))]
        return self

    def Delete(self):
        """
        Sets the query type to DELETE
        """
        assert self.query_type is None, \
            ".Delete() can not be called once query type has been set"
        self.query_type = DELETE
        return self

    def From(self, *args):
        """
        Fill in the list of tables in WHERE clause.

        Parameters are Tables or tuples of (Table, alias)
        where alias is a string
        """
        assert self.query_type in (SELECT, DELETE), \
            "From() is available only for Select() and Update() queries."
        for arg in args:
            if isinstance(arg, Table):
                self.from_tables.append(arg)
            elif isinstance(arg, Iterable) and isinstance(arg[0], Table):
                self.from_tables.append(arg[:2])
        return self

    def Where(self, *args):
        """
        Fill in the WHERE clause with conditions.
        They are expected to be of Expr type and are ANDed together
        """
        self.where_conds = reduce(operator.and_, args)
        return self

    def And(self, *args):
        """
        Add one or more AND condition to WHERE or HAVING clause,
        whichever is last
        If several Exprs passed, they are ANDed together
        and than ANDed to the right of current WHERE or HAVING
        """
        assert self.where_conds or self.having_conds, \
            ".And() can be called only after .Where() or .Having()"
        if not self.having_conds:
            self.where_conds &= reduce(operator.and_, args)
        else:
            self.having_conds &= reduce(operator.and_, args)
        return self

    def Or(self, *args):
        """
        Add one or more OR condition to WHERE or HAVING clause,
        whichever is last
        If several Exprs passed, they are ORed together
        and than ORed to the right of current WHERE or HAVING
        """
        assert self.where_conds or self.having_conds, \
            ".Or() can be called only after .Where() or .Having()"
        if not self.having_conds:
            self.where_conds |= reduce(operator.or_, args)
        else:
            self.having_conds |= reduce(operator.or_, args)
        return self

    def Join(self, table, join_type, *args):
        """
        Construct JOIN clause.

        Table is either a Table or a tuple of (Table, alias).
        join_type is a string, but also there are shortcut methods
            InnerJoin, OuterJoin, LeftJoin, RightJoin where it comes prefilled.
        Rest of parameters are join conditions. They are ANDed together.
        """
        self.joins.append({
            'table': table if isinstance(table, Table) else "%s %s" % table,
            'conds': reduce(operator.and_, args) if args else None,
            'type' : join_type,
            })
        return self
    def InnerJoin(self, table, *args):
        """
        Construct INNER JOIN.

        table is either a Table or a tuple of (Table, alias)
        Rest of parameters are join conditions. They are ANDed together.
        """
        return self.Join(table, "INNER", *args)
    def LeftJoin(self, table, *args):
        """
        Construct LEFT OUTER JOIN.

        table is either a Table or a tuple of (Table, alias)
        Rest of parameters are join conditions. They are ANDed together.
        """
        return self.Join(table, "LEFT OUTER", *args)
    def RightJoin(self, table, *args):
        """
        Construct RIGHT OUTER JOIN.

        table is either a Table or a tuple of (Table, alias)
        Rest of parameters are join conditions. They are ANDed together.
        """
        return self.Join(table, "RIGHT OUTER", *args)
    def OuterJoin(self, table, *args):
        """
        Construct OUTER JOIN.

        table is either a Table or a tuple of (Table, alias)
        Rest of parameters are join conditions. They are ANDed together.
        """
        return self.Join(table, "FULL OUTER", *args)

    def GroupBy(self, *args):
        """Construct GROUP BY clause. Parameters are Fields and Aliases."""
        self.group_fields = args
        return self

    def Having(self, *args):
        """Construct HAVING clause. Parameters are Exprs."""
        assert self.group_fields, "Having can only be used after GroupBy"
        self.having_conds = reduce(operator.and_, args)
        return self

    def OrderBy(self, *args):
        """Construct ORDER BY clause. Parameters are Fields and Aliases."""
        self.order_fields = args
        return self

    def Limit(self, num_rows):
        """Sets LIMIT BY clause. Parameter is a number"""
        self.limit = num_rows
        return self

    def sql(self, db=None):
        """Construct sql to be executed
        db parameter indicates type of database engine
        """
        opts = {'params': self.params, 'db': db}
        if self.query_type == UPDATE:
            assert self.set_fields, "No field setting rules issued, use Set()"
            res = "UPDATE %s SET %s" % (
                self.update_table,
                ", ".join(["%s = %s " %
                    (str(field), expr.sql(**opts))
                                    for (field, expr) in self.set_fields ]),
                )
        elif self.query_type == SELECT:
                res = "SELECT "
                if not self.select_fields:
                    res += "*"
                else:
                    str_fields = []
                    for f in self.select_fields:
                        if isinstance(f, (Field, Expr)):
                            str_fields.append(str(f))
                        elif isinstance(f, Table):
                            str_fields.append("%s.*" % str(f))
                        elif isinstance(f, Iterable):
                            str_fields.append("%s AS %s" % f)
                    res += ", ".join(str_fields)

        elif self.query_type == DELETE:
                res = "DELETE"
        else:
            raise Exception("Unknown query type")
        if self.query_type in (SELECT, DELETE):
            assert self.from_tables, "From() clause is required."
            res += " FROM %s" % ", ".join(
                [str(t) if isinstance(t, Table) else ("%s %s" % t)
                    for t in self.from_tables])
        if self.joins:
            for j in self.joins:
                res += " %(type)s JOIN %(table)s " % j
                if j['conds']:
                    res += "ON %s" % j['conds'].sql(**opts)

        if self.where_conds:
            res +=" WHERE %s" % self.where_conds.sql(**opts)

        if self.query_type == SELECT:
            if self.group_fields:
                res += " GROUP_BY %s" % (", ".join(
                    [str(field) for field in self.group_fields]))
            if self.having_conds:
                res +=" HAVING %s" % self.having_conds.sql(**opts)

        if self.limit:
            res +=" LIMIT %s" % self.limit
        return res

    def FetchFrom(self, db):
        """Actually execute the query.
        For SELECTs return ResultIterator for easy field retrieval
        """
        res = db._execute(self.sql(db=db._settings['engine']))
        if self.query_type == SELECT:
            return ResultIterator(self.select_fields, res)


class ResultIterator(object):
    """
    A wrapper over cursor returned from database,
    for each row returned from it, wraps it into RowWrapper,
    which allows accessing columns by their names or aliases.
    """
    def __init__(self, fields, cursor):
        """Initialize, pregenerate lowercase column name arrays."""
        self.short_fields = [field.name.lower() for field in fields]
        self.long_fields = [("%s__%s" % (str(field.table), field.name)).lower()
                 for field in fields]
        self.cursor = cursor

    def next(self):
        """Return RowWrapper for given row."""
        return RowWrapper(self.cursor.next(),
            self.short_fields, self.long_fields)

    def __iter__(self):
        """Indicate object as iterable"""
        return self

class RowWrapper(object):
    """
    A wrapper over cursor row returned from database.
    Allows accessing it by name, table__name and alias.
    """
    def __init__(self, values, short_fields, long_fields):
        """Initialize, save values and column name arrays."""
        self.values = values
        self.short_fields = short_fields
        self.long_fields = long_fields

    def __getattr__(self, attr):
        """
        Attempt to find given column name in our arrays.
        If successful, return it.
        """
        attr = attr.lower()
        if not self.values:
            return
        if attr in self.short_fields:
            return self.values[self.short_fields.index(attr)]
        if attr in self.long_fields:
            return self.values[self.long_fields.index(attr)]

    # this here to provide user with methods available in original value tuple
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
