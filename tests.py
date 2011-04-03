#!/usr/bin/env python

import sql
from sql import Expr as E

db = sql.Db()

#print repr(db.xx)
#print repr(db.Users)
#print repr(db.aa.bb)

#query = sql.SqlBuilder()
#print query.Select(db.Users.id, db.Users.login).From(db.Users).sql()
#print query.Select().From(db.Users).sql()
#print query.Select(db.Users.a, db.Users.b).From(db.Users).Where(db.Users.id == 4).sql()
#print query.Select(db.Users.a, db.Users.b).From(db.Users).Where(db.Users.id == 4).And(db.Users.name == 'Joe').sql()

def test_exprs():
    assert str(E(E('=', db.z.i, 1))) == "(z.i = 1)"
    assert repr(E(db.a.b == 1, db.b.c != 1).children) == \
        "[<Expr> (a.b = 1), <Expr> (b.c != 1)]"
    assert repr(E(db.a.b == 1, db.b.c != 1)) == "<Expr:AND>"
    assert str(E(db.a.b == 1, db.b.c != 1)) == "((a.b = 1) AND (b.c != 1))"
    assert str(E(db.a.b == 1, db.b.c != 1) & E(db.x.y == 'xx')) == \
        "((a.b = 1) AND (b.c != 1) AND (x.y = xx))"
    assert str(E(db.a.b== 1, db.b.c != 1) | E(db.x.y == 'xx')) == \
        "(((a.b = 1) AND (b.c != 1)) OR (x.y = xx))"
    assert str(E(db.a.b==1, db.b.c!=1) | E(db.x.y=='xx', db.d.y=='ee')) == \
        "(((a.b = 1) AND (b.c != 1)) OR ((x.y = xx) AND (d.y = ee)))"
    assert str(E(db.a.b==1, db.b.c!=1) & E(db.x.y=='xx', db.d.y=='ee')) == \
        "((a.b = 1) AND (b.c != 1) AND (x.y = xx) AND (d.y = ee))"
    assert str( (db.a.b == 1) & (db.b.c != 1) & ~(db.x.y == 'xx')) == \
        "((a.b = 1) AND (b.c != 1) AND NOT(x.y = xx))"


