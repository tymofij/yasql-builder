#!/usr/bin/env python

import sql
import datetime
from sql import Expr as E, Param as P, Literal as L
db = sql.Db(engine='sqlite', name=':memory:')

q = sql.SqlBuilder().Update(db.a).Set(
    (db.a.b, db.a.c + 4),(db.d.c, db.e.f * 8), simple_field = P('x')
    ).Where(db.g.h == P('h'))
q.params = {'x': 'XY', 'h': 'HH'}
print q.sql(db="sqlite")

def test_table_field_repr():
    assert repr(db.xx) == "<Table:xx>"
    assert repr(db.Users) == "<Table:Users>"
    assert repr(db.aa.bb) == "<Field:aa.bb>"

def test_exprs():
    # monkeypatch Literal to make it work without db provided
    sql.Literal.default_db = 'sqlite'
    # no new node should be created
    assert str(E(E(db.z.i, 1, operator='='))) == "(z.i = 1)"
    # representation of simple ones
    assert repr(E(db.a.b == 1, db.b.c != 1).children) == \
      "[<Expr: <Field:a.b> = <Literal:1>>, <Expr: <Field:b.c> != <Literal:1>>]"
    # a group is represented by its operator
    assert repr(E((db.a.b == 1) & (db.b.c != 1))) == "<Expr: AND>"
    # Literals can be passed in
    assert repr(E(1)) == "<Expr: <Literal:1>>"
    assert repr(E('x')) == "<Expr: <Literal:'x'>>"
    # simple stringification
    assert str(E((db.a.b > 1) & (db.b.c != 1))) == "((a.b > 1) AND (b.c != 1))"
    # merging in a leaf
    assert str(E((db.a.b >= 1) & (db.b.c != 1)) & E(db.x.y == 'xx')) == \
        "((a.b >= 1) AND (b.c != 1) AND (x.y = 'xx'))"
    # this leaf should not be merged in
    assert str(E((db.a.b < 1) & (db.b.c != 1)) | E(db.x.y == 'xx')) == \
        "(((a.b < 1) AND (b.c != 1)) OR (x.y = 'xx'))"
    # mergin in a multicond
    assert str(E((db.a.b <= 1) & (db.b.c!=1)) &
        E((db.x.y=='xx') & (db.d.y=='ee'))) == \
        "((a.b <= 1) AND (b.c != 1) AND (x.y = 'xx') AND (d.y = 'ee'))"
    # this multicond should not be merged in
    assert str(E((db.a.b==1) & (db.b.c!=1)) |
        E((db.x.y=='xx') & (db.d.y=='ee'))) == \
        "(((a.b = 1) AND (b.c != 1)) OR ((x.y = 'xx') AND (d.y = 'ee')))"
    # and this one also should not, because there is NOT
    assert str( (db.a.b == 1) & (db.b.c != 1) & ~(db.x.y == 'xx')) == \
        "((a.b = 1) AND (b.c != 1) AND NOT(x.y = 'xx'))"
    # return it to initial None
    sql.Literal.default_db = None

def test_params():
    opts = {'params': {'a': 'AAA', 'b': 'BBB', 'c': 'CCC', 1:111, 2: 222 },
            'db': 'sqlite'
        }
    # no new node should be created
    assert E(E(db.z.i, P('a'), operator='=')).sql(**opts) == "(z.i = 'AAA')"
    assert repr(P('x')) == "<Param:x>"
    assert repr(E(P('x'))) == "<Expr: <Param:x>>"
    # simple stringification
    assert (E((db.a.b == P('a')) & (db.b.c != P('b')))).sql(**opts) == \
        "((a.b = 'AAA') AND (b.c != 'BBB'))"
    # merging in a leaf
    assert (E((db.a.b == P(1)) & (db.b.c != P(2))) & E(db.x.y == P('c'))
        ).sql(**opts) == "((a.b = 111) AND (b.c != 222) AND (x.y = 'CCC'))"

def test_literals():
    assert L(-1).sql(db='sqlite') == '-1'
    assert L('X').sql(db='sqlite') == "'X'"
    assert L("'X").sql(db='sqlite') == "'''X'"
    assert L(r"'\\X").sql(db='mysql') == r"'''\\\\X'"
    assert L((1,2,3)).sql(db='sqlite') == "(1, 2, 3)"
    assert L((1,2,'x')).sql(db='sqlite') == "(1, 2, 'x')"
    assert L(datetime.date(year=2000, month=12, day=31)).sql(db='sqlite') == \
        "'2000-12-31'"
    assert L(datetime.time(hour=2, minute=12, second=6)).sql(db='sqlite') == \
        "'02:12:06'"
    assert L(datetime.datetime(
        year=1999, month=2, day=8, hour=2, minute=12, second=6)
        ).sql(db='sqlite') == "'1999-02-08 02:12:06'"
    assert L(None).sql(db='sqlite') == 'NULL'
    assert L(True).sql(db='mysql') == '1'
    assert L(True).sql(db='postgres') == "'t'"

def test_select():
    assert sql.SqlBuilder().Select(db.Users.id, db.Users.login).From(db.Users
        ).sql(db="sqlite") == "SELECT Users.id, Users.login FROM Users"
    assert sql.SqlBuilder().Select().From(db.Users
        ).sql(db="sqlite") == "SELECT * FROM Users"
    assert sql.SqlBuilder().Select(db.Users.id, db.Users.login).From(db.Users
        ).Where(db.Users.id == 4).sql(db="sqlite") == \
        "SELECT Users.id, Users.login FROM Users WHERE (Users.id = 4)"
    assert sql.SqlBuilder().Select(db.Users.id, db.Users.login).From(db.Users
        ).Where(db.Users.id == 4, db.Users.name == 'Joe').sql(db="sqlite"
        ) == "SELECT Users.id, Users.login FROM Users " \
             "WHERE ((Users.id = 4) AND (Users.name = 'Joe'))"

    assert sql.SqlBuilder().Select(db.Users.id, db.Users.login).From(db.Users
        ).Where(db.Users.id == 4).And(db.Users.name == 'Joe').sql(db="sqlite"
        ) == "SELECT Users.id, Users.login FROM Users " \
             "WHERE ((Users.id = 4) AND (Users.name = 'Joe'))"
    assert sql.SqlBuilder().Select(db.Users.id, db.Users.login).From(db.Users
        ).Where(db.Users.id == 4
        ).Or(db.Users.name == 'Joe', db.Users.name == 'Sarah').sql(db="sqlite"
        ) == "SELECT Users.id, Users.login FROM Users " \
             "WHERE ((Users.id = 4) OR ((Users.name = 'Joe') " \
             "OR (Users.name = 'Sarah')))"

def test_delete():
    assert sql.SqlBuilder().Delete().From(db.Users).sql(db="sqlite") == \
        "DELETE FROM Users"
    assert sql.SqlBuilder().Delete().From(db.Users
        ).Where(db.Users.id == 4).sql(db="sqlite") == \
        "DELETE FROM Users WHERE (Users.id = 4)"

def test_update():
    q = sql.SqlBuilder().Update(db.a).Set(
        (db.a.b, db.a.c + 4),(db.d.c, db.e.f * 8), simple_field = P('x')
        ).Where(db.g.h == P('h'))
    q.params = {'x': 'XY', 'h': 'HH'}
    assert q.sql(db="sqlite") == \
        "UPDATE a SET a.b = (a.c + 4) , d.c = (e.f * 8) , simple_field = 'XY'"\
        "  WHERE (g.h = 'HH')"
