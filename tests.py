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
    # no new node should be created
    assert str(E(E('=', db.z.i, 1))) == "(z.i = 1)"
    # representation of simple ones
    assert repr(E(db.a.b == 1, db.b.c != 1).children) == \
        "[<Expr> (a.b = 1), <Expr> (b.c != 1)]"
    # a group is represented by its operator
    assert repr(E(db.a.b == 1, db.b.c != 1)) == "<Expr:AND>"
    # simple stringification
    assert str(E(db.a.b == 1, db.b.c != 1)) == "((a.b = 1) AND (b.c != 1))"
    # merging in a leaf
    assert str(E(db.a.b == 1, db.b.c != 1) & E(db.x.y == 'xx')) == \
        "((a.b = 1) AND (b.c != 1) AND (x.y = xx))"
    # this leaf should not be merged in
    assert str(E(db.a.b== 1, db.b.c != 1) | E(db.x.y == 'xx')) == \
        "(((a.b = 1) AND (b.c != 1)) OR (x.y = xx))"
    # mergin in a multicond
    assert str(E(db.a.b==1, db.b.c!=1) & E(db.x.y=='xx', db.d.y=='ee')) == \
        "((a.b = 1) AND (b.c != 1) AND (x.y = xx) AND (d.y = ee))"
    # this multicond should not be merged in
    assert str(E(db.a.b==1, db.b.c!=1) | E(db.x.y=='xx', db.d.y=='ee')) == \
        "(((a.b = 1) AND (b.c != 1)) OR ((x.y = xx) AND (d.y = ee)))"
    # and this one also should not, because there is NOT
    assert str( (db.a.b == 1) & (db.b.c != 1) & ~(db.x.y == 'xx')) == \
        "((a.b = 1) AND (b.c != 1) AND NOT(x.y = xx))"
