Yet Another SQL Builder
-----------------------

Design and implement a module for working with some SQL datadase: building queries, fetching data, updating records, etc.

Usage should be the following:

```python
def GetUsersMapping(since):
    db = sql.Db(some_connection_parameters)
    query = sql.SqlBuilder()
    query.Select(db.Users.id, db.Users.login
        ).From(db.Users
        ).Where(db.Users.last_login_time < since
        ).And(db.Users.login != 'admin')
    rows = query.FetchFrom(db)
    return dict((row.id, row.login) for row in rows)
```

`sql.SqlBuilder` class should support:

- protection from SQL injection
- updating and deleting records
- selecting all fields from a table
- working with complex WHERE cases: `SELECT ... WHERE (flag = "A" OR (flag = "B" AND class = "m")) AND (position < 10)`
- SQL joins
- working with lists, sets and other sequential types in scope of SQL IN operator: `SELECT ... WHERE id IN (1, 2, 3, 4)`
- working with query parameters, that is an ability to create query once and then run it several times with different parameters

Nice to have:

- data types checking, i.e. it shouldn't be possible to do `...Where(db.Users.login < 5)` if `Users.login` contains strings.
- operations checking: `sql.Select(...).And(...).Where(...)` should lead to exception.

No special requirements for `sql.Db` class.

The module should be clearly and fully documented with docstrings, including methods' arguments and returning values.
Basic unittests are required.
