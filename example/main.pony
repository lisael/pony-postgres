"""
main.pony


"""
use "pg"
use "pg/codec"
use "pg/introspect"
use "net"
use "debug"
use "promises"


interface ConnectioHandler is Fulfill[Connection, Connection]

class BlogEntry
  let field1: I32
  let field2: I32
  let field3: I32

  new create(f1: I32, f2: I32, f3: I32) =>
    field1 = f1
    field2 = f2
    field3 = f3

class User
  let field1: I32
  new create(f1: I32) =>
    field1 = f1


class BlogEntryResultNotify
  let entries: Array[BlogEntry] = Array[BlogEntry]
  let view: BlogEntriesView tag
  new iso create(v: BlogEntriesView tag) => view = v
  fun iso descirption(desc: RowDescription) => None
  fun iso row(data: Array[PGValue]) => None
  fun iso stop() => None


class UserResultNotify
  let entries: Array[BlogEntry] = Array[BlogEntry]
  let view: BlogEntriesView tag
  new iso create(v: BlogEntriesView tag) => view = v
  fun iso descirption(desc: RowDescription) => None
  fun iso row(data: Array[PGValue]) => None
  fun iso stop() => None


actor BlogEntriesView
  var _conn: (Connection tag | None) = None
  var _user: ( User val | None ) = None
  let _entries: Promise[Array[BlogEntry iso] val] = Promise[Array[BlogEntry iso] val]

  be fetch_entries() =>
    try
      (_conn as Connection).fetch(
        "SELECT 1, 2, 3 UNION ALL SELECT 4, 5, 6 UNION ALL SELECT 7, 8, 9",
        recover BlogEntryResultNotify(this) end)
    end

  be fetch_user() =>
    try
      (_conn as Connection).fetch(
        "SELECT 1",
        recover BlogEntryResultNotify(this) end)
    end

  be render(entries: Array[BlogEntry iso] val) =>
    try (_conn as Connection).release() end
    

  be apply(conn: Connection tag) =>
    _conn = conn
    fetch_user()
    fetch_entries()
    _entries.next[BlogEntriesView](recover this~render() end)


actor Main
  let session: Session

  new create(env: Env) =>
    session = Session(env where user="macflytest",
                   password=EnvPasswordProvider(env),
                   database="macflytest")

    let that = recover tag this end
    session.execute("SELECT 42, 24 as foo;;",
             recover val
              lambda(r: Rows val)(that) =>
                  that.raw_handler(r)
              end
             end)

    session.execute("SELECT $1, $2 as foo",
                    recover val
                      lambda(r: Rows val)(that) =>
                        that.execute_handler(r)
                      end
                    end,
                   recover val [as PGValue: I32(70000), I32(-100000)] end)

    let p = session.connect(recover val
      lambda(c: Connection tag) =>
        BlogEntriesView(c)
      end
    end) 

  be raw_handler(rows: Rows val) =>
    for row in rows.values() do
      try Debug.out(row(0) as I32) end
      try Debug.out(row("foo") as I32) end
    end

  be execute_handler(rows: Rows val) =>
    for row in rows.values() do
      try Debug.out(row(0) as I32) end
      try Debug.out(row("foo") as I32) end
    end
    session.terminate()
