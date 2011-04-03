import copy

class Db(object):
    def __getattr__(self, name):
        return Table(name)

AND = "AND"
OR = "OR"

class Expr(object):
    negative = False
    children = [] # sub-expressions or literals
    operator = AND # what connects them
    parent = None

    def __new__(cls, *args):
        """
        creates new Expr object or returns existing,
        if called with first parameter of type Expr.
        in that case other parameters are ignored
        """
        if args:
            if isinstance(args[0], Expr): # one or more Exprs passed
                if len(args) > 1:
                    # several Expressions passed, add them as children
                    for c in args:
                        assert isinstance(c, Expr)
                    obj = object.__new__(cls)
                    obj.children = list(args)
                    obj.adopt_all()
                    return obj
                else: # just one Expr object
                    return args[0]
        # default object
        return object.__new__(cls)

    def __init__(self, first=None, *args):
        """ Can be initialized both as Expr("=", a, b) and Expr(Expr(..), c)"""
        if not isinstance(first, Expr): # that is handled in __new__
            self.operator = first
            self.children = list(args)
            self.adopt_all()

    def adopt_all(self):
        """ assigns self as a parent to all current children.
        mostly needed on create operations: __new__ and join
        """
        for child in self.children:
            if isinstance(child, (Param, Expr)):
                child.parent = self

    def join(self, other, operator):
        """
        joins this Expression with other one.
        if current is leaf object, then it is moved downwards as first child
        and other one is added as second child

        also that is performed when they are of different type etc.
        when possible through other is added to children list
        """
        assert isinstance(other, Expr)
        # if current is not leaf and merge seems possible
        if (self.operator == operator and not self.negative
                # other is either multicond of the same operation type,
                # non negated
                and (other.operator == operator and not other.negative
                    # or is a leaf:
                    or not other.is_multi()) ):
                # if multicond, we merge his kids with ours
                if other.is_multi():
                    self.children.extend(other.children)
                    self.adopt_all()
                # if a leaf, we add it to list of our children
                else:
                    other.parent = self
                    self.children.append(other)
                return self
        else:
            # shallowcopy ourselves into a new object, to be moved downside
            obj = copy.copy(self)
            obj.parent = self
            # that is brand new me, with new operator and kids
            self.operator = operator
            self.children = [obj, other]
            return self

    def is_multi(self):
        """ returns True when this Expr contains other Exprs """
        return [c for c in self.children if isinstance(c, Expr)]

    def __or__(self, other):
        return self.join(other, OR)

    def __and__(self, other):
        return self.join(other, AND)

    def __invert__(self):
        self.negative = not self.negative
        return self

    def __repr__(self):
        if self.is_multi(): # nested condition
            return ("<Expr: %s>" % self.operator).strip()
        else:
            return "<Expr: %s>" % (" %s " % self.operator).join([
                repr(child) for child in self.children])

    def __str__(self):
        return "%(not)s(%(expr)s)" % {
            'not': 'NOT' if self.negative else '',
            'expr':(" %s " % self.operator).join(
                        [str(c) for c in self.children])
            }

class Param(object):
    """ A parameter that can be passed to Expr and thus to SqlBuilder
    Looks for 'params' property dict in its parent object, then its parent
    and so on. Raises exception when parent with given parameter not found.
    """
    name = ''
    parent = None

    def __init__(self, name, parent=None):
        self.name = name
        self.parent = parent

    def __str__(self):
        parent = self.parent
        while (not hasattr(parent, 'params')
                                    or self.name not in parent.params):
            if parent is None or not hasattr(parent, 'parent'):
                raise Exception('parent with "%s" in params not found' %
                    self.name)
            parent = parent.parent
        return str(parent.params[self.name])

    def __repr__(self):
        return "<Param: %s>" % self.name


class Table(object):
    def __init__(self, name):
        # to avoid confusion with pretty common field 'name'
        self.__name = name

    def __repr__(self):
        return "<Table: %s>" % self.__name

    def __str__(self):
        return self.__name

    def __getattr__(self, name):
        return Field(self, name)


class Field(object):
    def __init__(self, table, name):
        self.table = table
        self.name = name

    def __str__(self):
        return "%s.%s" % (str(self.table), self.name)

    def __repr__(self):
        return "<Field %s>" % str(self)

    def __eq__(self, other):
        return Expr("=", self, other)

    def __ne__(self, other):
        return Expr("!=", self, other)


class SqlBuilder(object):
    """ the query builder """
    select_fields, from_tables, where_conds = None, None, None

    def Select(self, *args, **kwargs):
        self.select_fields = args
        return self

    def From(self, *args):
        self.from_tables = args
        return self

    def Where(self, *args):
        self.where_conds = list(args)
        return self

    def And(self, *args):
        self.where_conds.extend(args)
        return self

    def sql(self):
        """ Construct sql to be executed """
        res = "SELECT %(select_fields)s FROM %(from_tables)s" % {
            'from_tables':
                ", ".join([str(table) for table in self.from_tables]),
            'select_fields':
                ", ".join([str(field) for field in self.select_fields]) or "*"
        }
        if self.where_conds:
            res += " WHERE %s" % \
                 " AND ".join([str(cond) for cond in self.where_conds])

        return res

    def FetchFrom(self, db):
        pass
