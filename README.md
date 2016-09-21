# Pony-pg

Pure pony PostgreSQL client. Implements a connection pool and an actor-friendly
API.

## Status

Large proof of concept. May be subject to heavy changes or a complete rewrite.

- Connect to the database (only clear text password)
- Connection pool stub
- Send simple raw queries (no query parameters) and fetchs raw results
  in text
- Pluggable password fetching
  - RawPasswordProvider: to retieve a hard coded password or a raw string
  - EnvPasswordProvider: fetch the password from the standard PGPASSWORD
    env var
  - The PasswordProviders can be chained

All these featurea are half-baked at the moment. Status is 'sort-of-works' with
TODOs, Debug.out statements, poor logging  and poor error handling. The purpose
of this PoC is to experiment user-facing API and internal design alternatives.

## Planned feature 

### Needed in the proof of concept

- Prepared statements
- Codecs for values
  - binary values first
    - I prefer coding/decoding binary values than parsing text representation
  - The simple query protocol (used by raw queries) returns only text. We'll
    have to parse at some point.
- Binary result values
- Query parameters
  - Implemented using prepared statements, I don't want to implement escaping
    myself, too risky.
  - thus, raw queries are not likely to accept parameters any soon.

### Later...

- More auth options (at least MD5)
- Get and set connection parameters (these are currently fetched from the
  server but not exposed on the API)
- Transaction Management
- Cursors (prevents from loading the entire dataset of a result)
- Support logical decoding (I've already wrote a C/Python library for
  that, it's heavily asynchronous, it'll be a pleasure to do this in pony)

## Roadmap

### PoC

I'm currently working on prepared statements and a couple of codecs for a
couple of types.

### Design

Once it's done we'll have enough to start exploring designs and API. I plan to write
small psql-like client and a CRUD JSON-REST web app to chalange API designs.

### Release the best Postgres client library in the world.

TBD.

## Usage

The API is a moving target at the moment. Please read `example/main.pony`. This
program connects to a database (hard coded name) on localhost, using PGPASSWORD env var.

Make sure that in postgres' `pg_hba.conf` the authentication method is `password`, not `md5`

It can be compiled and run with:

```
make run
```

## Questions

Testing:
  No test at the momeent. It's bad, but the code is evolving too fast. A good test
  suite is a must-have for the Design phase.

For any question, ask me, `lisael` on freenode.net. I'm never far from #ponylang
