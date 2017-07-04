"""
main.pony
"""

use "pg"
use "pg/codec"
use "pg/introspect"
use "net"
use "logger"
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

  fun string(): String =>
    "BlogEntry " + field1.string() + " " + field2.string() + " " + field3.string()

class User
  let id: I32
  new create(id': I32) =>
    id = id'

  fun string(): String =>
    "User #" + id.string()


class BlogEntryRecordNotify is FetchNotify
  var entries: Array[BlogEntry val] trn = recover trn Array[BlogEntry val] end
  let view: BlogEntriesView tag

  new iso create(v: BlogEntriesView tag) =>
    view = v

  fun ref record(r: Record val) =>
    try
      let e = recover val BlogEntry(
        r(0) as I32,
        2, 3
        /*r(1) as I32,*/
        /*r(2) as I32*/
      ) end
      entries.push(e)
    end

  fun ref stop() =>
    let entries' = entries = recover trn Array[BlogEntry val] end
    view.entries(consume val entries')


class UserRecordNotify is FetchNotify
  let entries: Array[BlogEntry] = Array[BlogEntry]
  let view: BlogEntriesView tag
  let logger: Logger[String val] val

  new create(v: BlogEntriesView tag, out: OutStream) =>
    view = v
    logger = StringLogger(Fine, out)

  fun ref descirption(desc: RowDescription) => None

  fun ref batch(b: Array[Record val] val, next: FetchNotifyNext val) =>
    Debug(b.size())
    for r in b.values() do
      try
        view.user(recover User(r("id") as I32) end)
      else
        Debug.out("Error")
      end
    end

  fun ref record(r: Record val) =>
    try
      view.user(recover User(r("id") as I32) end)
    end

  fun ref stop() => None


actor BlogEntriesView
  var _conn: (Connection tag | None) = None
  var _user: ( User val | None ) = None
  let _entries: Promise[Array[BlogEntry val] val] = Promise[Array[BlogEntry val] val]
  let logger: Logger[String val] val
  let out: OutStream

  new create(o: OutStream) =>
    out = o
    logger = StringLogger(Fine, out)

  be fetch_entries() =>
    try
      Debug("fetch_entries")
      (_conn as Connection).fetch(
        /*"SELECT 1 as user_id, 2, 3 UNION ALL SELECT 4 as user_id, 5, 6 UNION ALL SELECT 7 as user_id, 8, 9",*/
        "SELECT generate_series(0,100)",
        recover BlogEntryRecordNotify(this) end)
    end

  be fetch_user() =>
    logger.log("fetch_user")
    try
      (_conn as Connection).fetch(
        "SELECT 1 as id",
        recover UserRecordNotify(this, out) end)
    end

  be user(u: User iso) =>
    logger.log("got user #" + u.id.string())
    _user = recover val consume u end
    Debug.out("###")
    fetch_entries()

  be render(entries': Array[BlogEntry val] val) =>
    Debug.out("render")
    logger.log("render")
    try logger.log(entries'.size().string() + " " + entries'(0).string()) end
    /*logger.log(entries'.size().string())*/
    try (_conn as Connection).release() end

  be entries(e: Array[BlogEntry val] val) =>
    Debug.out("fetch")
    _entries(e)
    
  be apply(conn: Connection tag) =>
    _conn = conn
    fetch_user()
    _entries.next[None](recover this~render() end)


interface iso Hydrate[A: Any #share]
  fun apply(record: Record val): A


class iso HydrateAll[A: Any #share]
  let _h: Hydrate[A] val

  new create(h: Hydrate[A] val) =>
    _h = h

  fun ref apply(records: Array[Record val] val): Array[A] val =>
    let result = recover trn Array[A] end
    for r in records.values() do
      result.push(_h(r))
    end
    consume val result


class iso HydrateOne[A: Any #share]
  let _h: Hydrate[A] val

  new create(h: Hydrate[A] val) =>
    _h = h

  fun ref apply(records: Array[Record val] val): A ? =>
    if records.size() != 1 then error end
    _h(records(0))


class SQLPromise[A: Any #share]
  let _query: String val
  let _sess: Session tag
  let promise: Promise[Array[Record val] val] = Promise[Array[Record val] val]
  var _sent: Bool = false
  var _h: Hydrate[A] val

  new iso create(query: String val, session: Session tag,
                 hydrate: Hydrate[A] val) =>
    _query = query
    _sess = session
    _h = hydrate

  fun ref _execute() =>
    if not _sent then
      Debug("execute")
      _sess.execute(_query, recover val {(records: Array[Record val] val) =>
        promise(records)
      } end)
      _sent = true
    end

  fun ref all[B: Any #share](
    fulfill: Fulfill[Array[A] val, B],
    rejected: Reject[B] = RejectAlways[B])
    : Promise[B] =>
    _execute()
    let result = promise.next[Array[A] val](recover HydrateAll[A](_h) end)
    result.next[B](consume fulfill, consume rejected)
    
  fun ref one[B: Any #share](
    fulfill: Fulfill[A, B],
    rejected: Reject[B] = RejectAlways[B])
    : Promise[B] =>
    Debug("One")
    _execute()
    let result = promise.next[A](recover HydrateOne[A](_h) end)
    result.next[B](consume fulfill, consume rejected)


primitive UserManager
  fun table(): String => "user"
  fun fields(): Array[String] val => recover val [
    "id"
  ] end
  fun hydrate(r: Record val): User val=>
    try
      recover val User(r("id") as I32) end
    else
      recover val User(42) end
    end

  fun by_id(id: I32, sess: Session): SQLPromise[User val]=>
    SQLPromise[User val]("SELECT 12 as id;", sess, recover UserManager~hydrate() end)
    

actor RequestContext
  let _user: SQLPromise[User val]

  new create(user_id: I32, sess: Session) =>
    _user = UserManager.by_id(user_id, sess)

  be user(callback: {(User val)} iso) =>
    Debug("call user")
    _user.one[None](consume callback)


actor BlogView
  let _ctx: RequestContext tag
  new create(ctx: RequestContext tag) =>
    _ctx = ctx

  be render(out: OutStream) =>
    _ctx.user(recover {(u: User val) => out.write("Hello " + u.string())} end)


actor Main
  let session: Session
  let _env: Env
  let logger: Logger[String val] val

  new create(env: Env) =>
    _env = env
    logger = StringLogger(Fine, env.out)
    session = Session(env where password=EnvPasswordProvider(env))
    let that = recover tag this end
    let bv = BlogView(RequestContext(1, session))
    bv.render(env.out)
    bv.render(env.out)

    """
    session.execute("SELECT generate_series(0,1)",
             recover val
              {(r: Rows val)(that) =>
                that.raw_count(r)
                None
              }
             end)

    session.execute("SELECT 42, 24 as foo;;",
             recover val
               {(r: Rows val)(that) =>
                 that.raw_handler(r)
               }
             end)


    session.execute("SELECT $1, $2 as foo",
                    recover val
                      {(r: Rows val)(that) =>
                        that.execute_handler(r)
                      }
                    end,
                   recover val [as PGValue: I32(70000); I32(-100000)] end)
  
    let p = session.connect(recover val
      {(c: Connection tag)(env) =>
        BlogEntriesView(env.out)(c)
      }
    end)
    """


  be raw_count(rows: Rows val) =>
    logger.log("rows: " + rows.size().string())

  be raw_handler(rows: Rows val) =>
    for row in rows.values() do
      try logger.log((row(0) as I32).string()) end
      try logger.log((row("foo") as I32).string()) end
    end

  be execute_handler(rows: Rows val) =>
    for row in rows.values() do
      try logger.log((row(0) as I32).string()) end
      try logger.log((row("foo") as I32).string()) end
    end
