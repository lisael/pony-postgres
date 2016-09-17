"""
pg.pony

Do pg stuff.
"""
use "net"
use "buffered"
use "collections"
use "promises"
use "debug"


trait Message
interface ClientMessage is Message
  fun ref _zero()
  fun ref _write(s: String) 
  fun ref _i32(i: I32)
  fun ref _done(id: U8): Array[ByteSeq] iso^
  fun ref done(): Array[ByteSeq] iso^ => _done(0)


class ClientMessageBase is ClientMessage
  var _w: Writer = Writer
  var _out: Writer = Writer

  fun ref _zero() => _w.u8(0)
  fun ref _u8(u: U8) => _w.u8(u)
  fun ref _write(s: String) => _w.write(s)
  fun ref _i32(i: I32) => _w.i32_be(i)
  fun ref _done(id: U8): Array[ByteSeq] iso^ =>
    if id != 0 then _out.u8(id) else Debug.out("Nope")end
    _out.i32_be(_w.size().i32() + 4)
    _out.writev(_w.done())
    _out.done()


class NullClientMessage is ClientMessage
  fun ref _zero() => None
  fun ref _write(s: String)  => None
  fun ref _i32(i: I32) => None
  fun ref _done(id: U8): Array[ByteSeq] iso^ => recover Array[ByteSeq] end


class StartupMessage is ClientMessage
  let _base: ClientMessageBase delegate ClientMessage = ClientMessageBase

  new create(params: Array[Param] box) =>
    _i32(196608) // protocol version 3.0
    for (key, value) in params.values() do
      add_param(key, value)
    end

  fun ref done(): Array[ByteSeq] iso^ => Debug.out("####"); _zero(); _done(0)

  fun ref add_param(key: String, value: String) =>
    _write(key); _zero()
    _write(value); _zero()


class TerminateMessage
  let _base: ClientMessageBase delegate ClientMessage = ClientMessageBase


class PasswordMessage is ClientMessage
  let _base: ClientMessageBase delegate ClientMessage = ClientMessageBase

  new create(pass: String) =>
    _write(pass)
    
  fun ref done(): Array[ByteSeq] iso^ => _done(112)



interface ServerMessage is Message

// pseudo messages
class ServerMessageBase is ServerMessage
class NullServerMessage is ServerMessage

// messages descirbed in https://www.postgresql.org/docs/current/static/protocol-message-formats.html
class AuthenticationOkMessage is ServerMessage
class ClearTextPwdRequest is ServerMessage
class MD5PwdRequest is ServerMessage
class ErrorMessage is ServerMessage
  let items: Array[(U8, Array[U8] val)] val
  new val create(it: Array[(U8, Array[U8] val)] val) =>
    items = it

type Param is (String, String)

class PGNotify is TCPConnectionNotify
  let _conn: Connection
  let _listener: Listener

  new iso create(c: Connection, l: Listener) =>
    _conn = c
    _listener = l

  fun ref connected(conn: TCPConnection ref) =>
    _conn.connected()

  fun ref received(conn: TCPConnection ref, data: Array[U8] iso) =>
    _listener.received(consume data)

actor Connection
  let _conn: TCPConnection tag
  let _pool: ConnectionPool tag
  let _listener: Listener tag
  let _params: Array[Param] val
  
  new create(auth: AmbientAuth,
             session: Session,
             host: String,
             service: String,
             params: Array[Param] val,
             pool: ConnectionPool) =>
    _listener = Listener(this)
    _conn = TCPConnection(auth, PGNotify(this, _listener), host, service)
    _pool = pool
    _params = params

  be writev(data: ByteSeqIter) =>
    _conn.writev(data)

  be connected() =>
		log("connected")
    let data = recover val
    let msg = StartupMessage(_params)
    msg.done() 
    end
    _conn.writev(data)

  be log(msg: String) => _pool.log(msg)


actor ConnectionPool
  let _connections: Array[Connection] = Array[Connection tag]
  let _params: Array[Param] val
  let _sess: Session tag
  let _host: String
  let _service: String

  new create(session: Session tag, host: String, service: String, params: Array[Param] val) =>
    _params = params
    _sess = session
    _host = host
    _service = service

  be log(msg: String) =>
    _sess.log(msg)

  be connect(auth: AmbientAuth) =>
    let conn = Connection(auth, _sess, _host, _service, _params, this) 

  be got_pass(pass: String) =>
    None

  be get_pass() =>
    Debug.out("get_pass")
    got_pass("hop")
     

type PGDo[B: Any #share] is Fulfill[Connection, B]
type PGPromise is Promise[Connection]
  

actor Session
  let _env: Env
  let _pool: ConnectionPool

  new create(env: Env,
             host: String="",
             service: String="",
             user: String="",
             password: String = "",
             database: String = "") =>
    _env = env
    _pool = ConnectionPool(this, host, service,
      recover val [("user", user), ("database", database)] end)

  be log(msg: String) =>
    _env.out.print(msg)

  be connect() =>
    """Create a connection and try to log in"""
    try _pool.connect(_env.root as AmbientAuth) end

  be connected() => None

  be terminate()=> None

/*
class StartupHandler is Handler
  let _base: HandlerBase delegate Handler
  let _params: Array[Param] val

  new create(p: ConnectionPoolOld, params: Array[Param] val) =>
    _base = HandlerBase(p)
    _params = params

  fun ref connected(conn: TCPConnection ref) =>
		log("connected")
    let data = recover val
    let msgi = StartupMessage(_params)
    msgi.done() 
    end
    conn.writev(data)

  fun ref received(conn: TCPConnection ref, data': Array[U8] iso) =>
    match filter(consume data')
    | None => None
    | let r: ClearTextPwdRequest val =>
      pool().get_pass(this)
    | let r: MD5PwdRequest val  =>
      pool().get_pass(this)
    else
      log("Unknown ServerMessage")
    end
  
    














actor ConnectionPoolOld
  let _ready: Array[TCPConnection] = Array[TCPConnection tag]
  let _params: Array[Param] val
  let _auth: (AmbientAuth | None)
  let _sess: Session
  var _connections: MapIs[Handler tag, TCPConnection] = MapIs[Handler tag, TCPConnection]

  new create(auth: (AmbientAuth | None), session: Session tag, params: Array[Param] val) =>
    _auth = auth
    _params = params
    _sess = session

  be handle(h: Handler iso, old: None = None) =>
    let t: Handler tag = recover tag h end
    try
      let conn = TCPConnection(_auth as AmbientAuth, consume h, "", "5432")
      _connections.insert(t, conn)
    end

  be handle(h: Handler iso, old: Handler tag) =>
    try
      let data: Array[ByteSeq] val = h.data()
      /*for s in data.values() do*/
        /*match s*/
        /*| let s': Array[U8 val] val =>*/
          /*for c in s'.values() do*/
            /*log(c.string())*/
          /*end*/
        /*| let s': String =>*/
          /*for c in s'.values() do*/
            /*log(c.string())*/
          /*end*/
        /*end*/
      /*end*/
      let conn = _connections(old)
      _connections.insert(h, conn)
      conn.set_notify(consume h)
      _connections.remove(old)
      conn.writev(data)
    end

  be log(msg: String) =>
    _sess.log(msg)

  be connect() =>
    None
    /*let h: Handler iso = recover StartupHandler(this, _params) end*/
    /*handle(consume h)*/

  be got_pass(pass: String, h: Handler tag) =>
    Debug.out("got_pass")
    handle(recover PasswordHandler(this, pass) end, h)

  be get_pass(h: Handler tag) =>
    Debug.out("get_pass")
    got_pass("hop", h)
     

trait Handler is TCPConnectionNotify
  fun log(msg: String)
  fun pool(): ConnectionPoolOld
  fun ref data(): Array[ByteSeq] iso^
  fun ref set_data(d: Array[ByteSeq] iso)
  fun box filter(data': Array[U8] iso): (ServerMessage val | None)


class HandlerBase is Handler
  let _pool: ConnectionPoolOld
  var _data: Array[ByteSeq] iso = recover Array[ByteSeq] end
  
  new create(pool': ConnectionPoolOld) => _pool = pool'
  fun log(msg: String) => _pool.log(msg)
  fun pool(): ConnectionPoolOld => _pool
  fun ref data(): Array[ByteSeq] iso^ => _data = recover Array[ByteSeq] end
  fun ref set_data(d: Array[ByteSeq] iso) => _data = consume d
  fun box filter(data': Array[U8] iso): (ServerMessage val | None) =>
    match ParseResponse(consume data')
    | let r: ParseError val => log(r.msg)
    | let r: ErrorMessage val =>
      log("Error:")
      for (typ, value) in r.items.values() do
        log("  " + typ.string() + ": " + String.from_array(value))
      end
    | let r: ServerMessage val => r
    end


class PasswordHandler is Handler
  let _base: HandlerBase delegate Handler

  new create(p: ConnectionPoolOld, pass: String) =>
    _base = HandlerBase(p)
    set_data(PasswordMessage(pass).done())

  fun ref received(conn: TCPConnection ref, data': Array[U8] iso) =>
    match filter(consume data')
    | None => None
    | let r: ClearTextPwdRequest val =>
      pool().get_pass(this)
    | let r: MD5PwdRequest val  =>
      pool().get_pass(this)
    else
      log("Unknown ServerMessage")
    end

*/
