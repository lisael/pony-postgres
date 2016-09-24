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

class Rows
  let _rows: Array[Array[FieldData val] val] = Array[Array[FieldData val]val]
  let desc: RowDescription val

  new create(d: RowDescription val) => desc = d
  fun ref append(d: Array[FieldData val]val) => _rows.push(d)
  fun fields(): Iterator[Array[FieldData val] val] => _rows.values()
  fun values(): Iterator[Array[PGValue val]] =>
    let it = _rows.slice().values()
    PGValueIterator(consume it, desc)
  //fun as_maps()


interface RowsCB
  fun apply(iter: Rows)

type Param is (String, String)

actor Connection
  let _conn: BEConnection tag

  new create(c: BEConnection tag) =>
    _conn = c

  be raw(q: String, handler: RowsCB val) =>
    _conn.raw(q, handler)

  be do_terminate() =>
    Debug.out("Bye")

     
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

    _mgr = ConnectionManager(this, host, service, user', provider, 
      recover val [("user", user'), ("database", database)] end)

  be log(msg: String) =>
    _env.out.print(msg)

  be connect() =>
    """Create a connection and try to log in"""
    None
    /*try _pool.connect(_env.root as AmbientAuth) end*/

  be raw(q: String, handler: RowsCB val) =>
    try
      let f = recover lambda(c: Connection)(q, handler) => c.raw(q, handler) end end
      _mgr.connect(_env.root as AmbientAuth, consume f)
    end

  be connected() => None

  be terminate()=>
    _mgr.terminate()

