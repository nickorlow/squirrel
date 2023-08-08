# SQUIRREL
SQL Query Util-Izing Rust's Reliable and Efficient Logic

![Demo showing data insertation into SQUIRREL](https://github.com/nickorlow/squirrel/blob/main/.meta/images/demo_data_insert.png?raw=true)

---

## About
This is a SQL database written in Rust. It will be based off of (and hopefully be made wire-compatible with) PostgreSQL's syntax.

## Feature roadmap

[X] CREATE TABLE with varchar & integer datatypes

[X] INSERT INTO (non-batched)

[X] SELECT * query 

[ ] DELETE command

[ ] SELECT (filtered columns) query 

[ ] WHERE clause for SELECT and DELETE

[ ] Primary Keys via B+ Tree

[ ] Foreign Keys

[ ] Some form of JOINs

[ ] Support [Postgres' messaging system](https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.6.7.3) (wire compatability)
 
... other stuff is TBD
