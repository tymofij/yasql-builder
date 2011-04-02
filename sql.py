class Db(object):
    def __getattr__(self, name):
        return Table(name)

AND = " AND "
OR = " OR "

class Cond(object):
    negative = False
    children = [] # subconditions
    operator = AND # what connects them

    def __new__(cls, *args):
        """
        creates new Cond object or returns existing,
        if called with first parameter of type Cond.
        in that case other parameters are ignored
        """
        if args:
            if isinstance(args[0], Cond): # one or more Conds passed
                if len(args) > 1:
                    # several conditions passed, add them as children
                    for c in args:
                        assert isinstance(c, Cond)
                    obj = object.__new__(cls)
                    obj.children = list(args)
                    return obj
                else: # just one Cond object
                    return args[0]
        # default object
        return object.__new__(cls)

    def __init__(self, first=None, *args):
        """ Can be initialized both as Cond(a, "=", b) and Cond(Cond(..), c)"""
        if not isinstance(first, Cond):
            if first:
                assert isinstance(first, Field)
                self.first = first
                if args:
                    assert len(args) == 2
                self.operator, self.second = args

    def add(self, other, operator):
        if other in self.children and operator == self.connector:
            return self
        if len(self.children) < 2:
            self.operator = operator
        if self.operator == operator:
            # TODO: could be nicer
            if isinstance(other, Cond) and not self.negative and \
                (other.operator == operator or len(other.children) == 1):
                self.children.extend(other.children)
                return self
            else:
                self.children.append(other)
                return self
        else:
            obj = type(self)()
            obj.children = self.children
            obj.operator = self.operator
            obj.negative = self.negative

            self.operator = operator
            self.children = [obj, other]
            return self

    def __or__(self, other):
        return self.add(other, OR)

    def __and__(self, other):
        return self.add(other, AND)

    def __invert__(self):
        self.negative = not self.negative
        return self

    def __repr__(self):
        if self.children:
            return ("<Cond:%s>" % self.operator).strip()
        else:
            return "<Cond> %s" % str(self)

    def __str__(self):
        if self.children:
            return "%(not)s(%(expr)s)" % {
                'not': 'NOT' if self.negative else '',
                'expr':self.operator.join([str(c) for c in self.children])
                }
        else:
            expr = "%s %s %s" % \
                (str(self.first), str(self.operator), str(self.second))
            return 'NOT(%s)' % expr if self.negative else expr


class Table(object):
    def __init__(self, name):
        # to avoid confusion with pretty common field 'name'
        self.__name = name

    def __repr__(self):
        return "<Table> %s" % self.__name

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
        return "<Field> %s" % str(self)

    def __eq__(self, other):
        return Cond(self, "=", other)

    def __ne__(self, other):
        return Cond(self, "!=", other)


class SqlBuilder(object):
    """the query builder"""
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

    def FetchFrom(self, db):
        """Go!"""
        res = "SELECT %(select_fields)s FROM %(from_tables)s" % {
            'from_tables':
                ", ".join([str(table) for table in self.from_tables]),
            'select_fields':
                ", ".join([str(field) for field in self.select_fields]) or "*"
        }
        if self.where_conds:
            res += " WHERE %s" % \
                 " AND ".join([str(cond) for cond in self.where_conds])

        print res
