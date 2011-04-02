class Db:
    def __getattr__(self, name):
        return Table(name)

class Table:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<Table> %s" % self.name

    def __str__(self):
        return self.name

    def __getattr__(self, name):
        return Field(self, name)


class Field:
    def __init__(self, table, name):
        self.table = table
        self.name = name

    def __str__(self):
        return "%s.%s" % (self.table.name, self.name)

    def __repr__(self):
        return "<Field> %s" % str(self)


class SqlBuilder:
    """the query builder"""

    def Select(self, *args, **kwargs):
        self.select_fields = args
        return self

    def From(self, *args):
        self.from_tables = args
        return self

    def FetchFrom(self, db):
        """Go!"""
        print "SELECT %(select_fields)s FROM %(from_tables)s" % {
            'from_tables': ", ".join([str(table) for table in self.from_tables]),
            'select_fields': ", ".join([str(field) for field in self.select_fields]) or "*"
        }
