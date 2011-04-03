#!/usr/bin/env python

import sql
from sql import Expr as E

db = sql.Db()

"""
print repr(db.xx)
print repr(db.Users)
print repr(db.aa.bb)

query = sql.SqlBuilder()
query.Select(db.Users.id, db.Users.login).From(db.Users)
query.FetchFrom(db)

query.Select().From(db.Users)
query.FetchFrom(db)

query.Select(db.Users.a, db.Users.b).From(db.Users).Where(db.Users.id == 4)
query.FetchFrom(db)

query.Select(db.Users.a, db.Users.b).From(db.Users).Where(db.Users.id == 4).And(db.Users.name == 'Joe')
query.FetchFrom(db)


print E(E(db.z.i, '=', 1))
print E(db.a.b == 1, db.b.c != 1).children
print repr(E(db.a.b == 1, db.b.c != 1))
print str(E(db.a.b == 1, db.b.c != 1))

print str(E(db.a.b == 1, db.b.c != 1) & E(db.x.y == 'xx'))
print str(E(db.a.b == 1, db.b.c != 1) | E(db.x.y == 'xx'))
print str(E(db.a.b == 1, db.b.c != 1) | E(db.x.y == 'xx', db.d.y == 'ee'))
print E(db.a.b == 1, db.b.c != 1) & E(db.x.y == 'xx', db.d.y == 'ee')

"""

print (db.a.b == 1) & (db.b.c != 1) & ~(db.x.y == 'xx')
#print E(db.a.b == 1)
