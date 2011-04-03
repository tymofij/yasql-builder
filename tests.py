#!/usr/bin/env python

import sql
from sql import Expr as E, Param as P
db = sql.Db()

params = {'a': 'AAA', 'b': 'BBB', 'c': 'CCC', 1:111, 2: 222, 'x': 'XXX' }
e = E(db.a.b == P(1), db.b.c != P(2)) & E(db.x.y == P('x'))
e.params = params
print e

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
        "[<Expr: <Field a.b> = 1>, <Expr: <Field b.c> != 1>]"
    # a group is represented by its operator
    assert repr(E(db.a.b == 1, db.b.c != 1)) == "<Expr: AND>"
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

def test_params():
    params = {'a': 'AAA', 'b': 'BBB', 'c': 'CCC', 1:111, 2: 222, 'x': 'XXX' }
    # no new node should be created
    e = E(E('=', db.z.i, P('a')))
    e.params = params
    assert str(e) == "(z.i = AAA)"
    assert repr(P('x')) == "<Param: x>"
    # simple stringification
    e = E(db.a.b == P('a'), db.b.c != P('b'))
    e.params = params
    assert str(e) == "((a.b = AAA) AND (b.c != BBB))"
    # merging in a leaf
    e = E(db.a.b == P(1), db.b.c != P(2)) & E(db.x.y == P('x'))
    e.params = params
    assert str(e) == "((a.b = 111) AND (b.c != 222) AND (x.y = XXX))"
#    # this leaf should not be merged in
#    assert str(E(db.a.b== 1, db.b.c != 1) | E(db.x.y == 'xx')) == \
#        "(((a.b = 1) AND (b.c != 1)) OR (x.y = xx))"
#    # mergin in a multicond
#    assert str(E(db.a.b==1, db.b.c!=1) & E(db.x.y=='xx', db.d.y=='ee')) == \
#        "((a.b = 1) AND (b.c != 1) AND (x.y = xx) AND (d.y = ee))"
#    # this multicond should not be merged in
#    assert str(E(db.a.b==1, db.b.c!=1) | E(db.x.y=='xx', db.d.y=='ee')) == \
#        "(((a.b = 1) AND (b.c != 1)) OR ((x.y = xx) AND (d.y = ee)))"
#    # and this one also should not, because there is NOT
#    assert str( (db.a.b == 1) & (db.b.c != 1) & ~(db.x.y == 'xx')) == \
#        "((a.b = 1) AND (b.c != 1) AND NOT(x.y = xx))"
