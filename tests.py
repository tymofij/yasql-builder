#!/usr/bin/env python

import sql

db = sql.Db()
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

print sql.Cond(sql.Cond(db.z.i, '=', 1))
print sql.Cond(db.a.b == 1, db.b.c != 1).children
print repr(sql.Cond(db.a.b == 1, db.b.c != 1))
print str(sql.Cond(db.a.b == 1, db.b.c != 1))

print str(sql.Cond(db.a.b == 1, db.b.c != 1) & sql.Cond(db.x.y == 'xx'))
print str(sql.Cond(db.a.b == 1, db.b.c != 1) | sql.Cond(db.x.y == 'xx'))
