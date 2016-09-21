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

interface StringCB
  fun apply(s: String)
interface PassCB is StringCB
interface UserCB is StringCB

class Rows
  let _rows: Array[Array[FieldData val] val] = Array[Array[FieldData val]val]
  let _desc: RowDescription val

  new create(d: RowDescription val) => _desc = d

  fun ref append(d: Array[FieldData val]val) => _rows.push(d)

  fun values(): Iterator[Array[FieldData val] val] => _rows.values()


interface RowsCB
  fun apply(iter: Rows)


type Param is (String, String)




class PGNotify is TCPConnectionNotify
  let _conn: _Connection
  let _listener: Listener

  new iso create(c: _Connection, l: Listener) =>
    _conn = c
    _listener = l

  fun ref connected(conn: TCPConnection ref) =>
    _conn.connected()

  fun ref received(conn: TCPConnection ref, data: Array[U8] iso) =>
    _listener.received(consume data)

  fun ref closed(conn: TCPConnection ref) =>
    _listener.received(recover [as U8: 0, 0, 0, 0, 0]end)
    _conn.received(ConnectionClosedMessage)

actor _Connection
  let _conn: TCPConnection tag
  var _fe: (Connection tag | None) = None // front-end connection
  let _pool: _ConnectionPool tag
  let _listener: Listener tag
  let _params: Array[Param] val
  var _convs: List[_Conversation tag] = List[_Conversation tag]
  var _current: _Conversation tag
  var _backend_key: (U32, U32) = (0, 0)
  
  new create(auth: AmbientAuth,
             session: Session,
             host: String,
             service: String,
             params: Array[Param] val,
             pool: _ConnectionPool) =>
    _listener = Listener(this)
    _conn = TCPConnection(auth, PGNotify(this, _listener), host, service)
    _pool = pool
    _params = params
    _current = _AuthConversation(_pool, this, _params)

  be writev(data: ByteSeqIter) =>
    _conn.writev(data)

  fun ref _schedule(conv: _Conversation tag) =>
    match _current
    | let n: _NullConversation =>
      _current = conv
      _current(this)
    else
      _convs.push(conv)
    end

  be schedule(conv: _Conversation tag) =>
    _schedule(conv)

  be connected() =>
    _current(this)

  be _set_backend_key(m: BackendKeyDataMessage val) =>
    Debug("set backend key")
    _backend_key = m.data

  be log(msg: String) => _pool.log(msg)

  be next() =>
    try
      _current = _convs.shift()
      _current(this)
    else
      _current = _NullConversation(this)
    end

  be update_param(p: ParameterStatusMessage val) =>
    // TODO: update the parameters and allow the user to query them
    Debug.out("Update param " + p.key + " " + p.value)

  be received(s: ServerMessage val) =>
    _current.message(s)

  be _log_error(m: ErrorMessage val) =>
    for (tagg, text) in m.items.values() do
      let s: String trn = recover trn String(text.size() + 3) end
      s.push(tagg)
      s.append(": ")
      s.append(text)
      log(consume s)
    end

  be handle_message(s: ServerMessage val) =>
    match s
    | let m: ParameterStatusMessage val => update_param(m)
    | let m: BackendKeyDataMessage val => _set_backend_key(m)
    | let m: ErrorMessage val => _log_error(m)
    | let m: ConnectionClosedMessage val => log("Disconected")
    else
      log("Unknown ServerMessage")
    end

  be raw(q: String, handler: RowsCB val) =>
    schedule(_QueryConversation(q, this, handler))

  be terminate() =>
    schedule(_TerminateConversation(this))

  be do_terminate() =>
    Debug.out("######")
    try (_fe as Connection).do_terminate() end

  be set_frontend(c: Connection tag) =>
    _fe = c


actor Connection
  let _conn: _Connection tag

  new _create(c: _Connection) =>
    _conn = c

  be raw(q: String, handler: RowsCB val) =>
    _conn.raw(q, handler)

  be do_terminate() =>
    Debug.out("Bye")

actor _ConnectionPool
  let _connections: Array[_Connection] = Array[_Connection tag]
  let _params: Array[Param] val
  let _sess: Session tag
  let _host: String
  let _service: String
  let _user: String
  let _passwd_provider: PasswordProvider tag
  var _password: (String | None) = None

  new create(session: Session tag,
             host: String,
             service: String,
             user: String,
             passwd_provider: PasswordProvider tag,
             params: Array[Param] val) =>
    _params = params
    _sess = session
    _host = host
    _service = service
    _passwd_provider = passwd_provider
    _user = user

  be log(msg: String) =>
    _sess.log(msg)

  be connect(auth: AmbientAuth, f: {(Connection tag)} iso) =>
    let priv_conn=_Connection(auth, _sess, _host, _service, _params, this)
    _connections.push(priv_conn)
    let conn = Connection._create(priv_conn)
    priv_conn.set_frontend(conn)
    f(conn)

  be get_pass(f: PassCB iso) =>
    // TODO: implement a versatile get_pass function
    _passwd_provider(consume f)

  be get_user(f: UserCB iso) =>
    f(_user)

  be terminate() =>
    for i in Range(0, _connections.size()) do
      try _connections.pop().terminate() end
    end
     
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
  let _mgr: _ConnectionPool

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

    _mgr = _ConnectionPool(this, host, service, user', provider, 
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

