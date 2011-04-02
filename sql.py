class Db(object):
    def __getattr__(self, name):
        return Table(name)


class Cond(object):
    def __init__(self, first, operator, second):
        self.first = first
        self.operator = operator
        self.second = second

    def __str__(self):
        return "%s %s %s" % \
            (str(self.first), str(self.operator), str(self.second))


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
