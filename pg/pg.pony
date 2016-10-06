"""
pg.pony

Do pg stuff.
"""
use "net"
use "buffered"
use "collections"
use "promises"
use "options"
use "debug"

use "pg/protocol"
use "pg/introspect"
use "pg/connection"
use "pg/codec"


interface StringCB
  fun apply(s: String)
interface PassCB is StringCB
interface UserCB is StringCB


type PGValue is (I64 | I32 | None)

class PGValueIterator is Iterator[Array[PGValue val]]
  let _it: Iterator[Array[FieldData val] val]
  let _desc: RowDescription val

  new create(it: Iterator[Array[FieldData val] val] ref, desc: RowDescription val) =>
    _it = it
    _desc = desc

  fun ref has_next(): Bool => _it.has_next()

  fun ref next(): Array[PGValue] ? =>
    let result = Array[PGValue](_desc.fields.size())
    var idx = USize(0)
    for value in _it.next().values() do
      let typ = _desc.fields(idx).type_oid
      let fmt = _desc.fields(idx).format
      idx = idx + 1
      result.push(Decode(typ, value.data, fmt))
    end
    result

class Result
  let _desc: TupleDescription val
  let _tuple: Array[FieldData val] val

  new create(d: TupleDescription val, t: Array[FieldData val] val ) =>
    _desc = d
    _tuple = t

  fun apply(idx: ( USize | String )): PGValue ? =>
    (let pos: USize, let d: FieldDescription val) = _desc(idx) 
    Decode(d.type_oid, _tuple(pos).data, d.format)

interface ResultCB
  fun apply(iter: Array[Result val] val)

type Param is (String, String)

type Rows is Array[Result val]

actor Connection
  let _conn: BEConnection tag

  new create(c: BEConnection tag) =>
    _conn = c

  be raw(q: String, handler: ResultCB val) =>
    _conn.raw(q, handler)

  be execute(query: String, params: Array[PGValue] val, handler: ResultCB val) =>
    _conn.execute(query, params, handler)

  be terminate() =>
    _conn.terminate()

  be do_terminate() =>
    Debug.out("Bye")

  be cursor(query: String, notify: CursorNotify iso) =>
    Debug.out("######### Cursor ############")

     
interface PasswordProvider
  be apply(f: PassCB val)
  be chain(p: PasswordProvider tag)

actor RawPasswordProvider
  let _password: String

  new create(p: String) => _password = p
  be apply(f: PassCB val) => f(_password)
  be chain(p: PasswordProvider tag) => None

actor EnvPasswordProvider
  let _env: Env
  var _next: (PasswordProvider tag | None) = None

  new create(e: Env) => _env = e
  be chain(p: PasswordProvider tag) => _next = p
  be apply(f: PassCB val) =>
    try
      f(EnvVars(_env.vars())("PGPASSWORD"))
    else
      try (_next as PasswordProvider tag)(f) end
    end

actor Session
  let _env: Env
  let _mgr: ConnectionManager

  new create(env: Env,
             host: String="",
             service: String="5432",
             user: (String | None) = None,
             password: (String | PasswordProvider tag) = "",
             database: String = "") =>
    _env = env

    // retreive the user from the env if not provided
    let user' = try user as String else
      try EnvVars(env.vars())("USER") else "" end
    end

    // Define the password strategy
    let provider = match password
      | let p: PasswordProvider tag => p
      | let s: String => RawPasswordProvider(s)
      else
        RawPasswordProvider("")
      end

    _mgr = ConnectionManager(host, service, user', provider, 
      recover val [("user", user'), ("database", database)] end)

  be log(msg: String) =>
    _env.out.print(msg)

  be connect(f: {(Connection tag)} val) =>
    try _mgr.connect(_env.root as AmbientAuth, f) end

  be raw(query: String, handler: ResultCB val) =>
    try
      let f = recover lambda(c: Connection)(query, handler) =>
          c.raw(query, handler)
        end
      end
      _mgr.connect(_env.root as AmbientAuth, consume f)
    end

    
  be execute(query: String, params: Array[PGValue] val, handler: ResultCB val) =>
    try
      let f = recover lambda(c: Connection)(query, params, handler) =>
          c.execute(query, params, handler)
        end
      end
      _mgr.connect(_env.root as AmbientAuth, consume f)
    end

  be terminate()=>
    _mgr.terminate()

