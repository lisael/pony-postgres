"""
pg.pony

Do pg stuff.
"""
use "net"
use "buffered"
use "collections"
use "promises"
use "debug"

interface StringCB
  fun apply(s: String)
interface PassCB is StringCB
interface UserCB is StringCB

primitive IdleTransction
primitive ActiveTransaction
primitive ErrorTransaction
primitive UnknownTransactionStatus

type TransactionStatus is (IdleTransction
                          | ActiveTransaction
                          | ErrorTransaction
                          | UnknownTransactionStatus)

primitive _StatusFromByte
  fun apply(b: U8): TransactionStatus =>
    match b
    | 73 => IdleTransction // I
    | 84 => ActiveTransaction // T
    | 69 => ErrorTransaction // E
    else
      UnknownTransactionStatus
    end

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
    if id != 0 then _out.u8(id) end
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
  let salt: Array[U8] val
  new val create(salt': Array[U8] val)=>
    salt = salt'
class ErrorMessage is ServerMessage
  let items: Array[(U8, Array[U8] val)] val
  new val create(it: Array[(U8, Array[U8] val)] val) =>
    items = it

class ParameterStatusMessage is ServerMessage
  let key: String val
  let value: String val
  new val create(k: Array[U8] val, v: Array[U8] val) =>
    key = String.from_array(k)
    value = String.from_array(v)

class ReadyForQueryMessage is ServerMessage
  let status: TransactionStatus
  new val create(b: U8) =>
    status = _StatusFromByte(b)

class BackendKeyDataMessage is ServerMessage
  let data: (U32, U32)
  new val create(pid: U32, key: U32) =>
    data = (pid,key)

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
  var _interaction: Interaction tag
  var _backend_key: (U32, U32) = (0, 0)
  
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
    _interaction = AuthInteraction(_pool, this, _params)

  be writev(data: ByteSeqIter) =>
    _conn.writev(data)

  be connected() =>
    _interaction(this)

  be _set_backend_key(m: BackendKeyDataMessage val) =>
    Debug("set backend key")
    _backend_key = m.data

  be log(msg: String) => _pool.log(msg)

  be update_param(p: ParameterStatusMessage val) =>
    // TODO
    Debug.out("Update param " + p.key + " " + p.value)

  be received(s: ServerMessage val) =>
    match s
    | let m: ParameterStatusMessage val => update_param(m)
    | let m: ReadyForQueryMessage val => None
    | let m: BackendKeyDataMessage val => _set_backend_key(m)
    else
      _interaction.message(s)
    end


actor ConnectionPool
  let _connections: Array[Connection] = Array[Connection tag]
  let _params: Array[Param] val
  let _sess: Session tag
  let _host: String
  let _service: String
  let _user: String

  new create(session: Session tag, host: String, service: String, user: String, params: Array[Param] val) =>
    _params = params
    _sess = session
    _host = host
    _service = service
    _user = user

  be log(msg: String) =>
    _sess.log(msg)

  be connect(auth: AmbientAuth) =>
    log("connecting")
    let conn = Connection(auth, _sess, _host, _service, _params, this) 

  be got_pass(pass: String, f: PassCB iso) =>
    f(pass)

  be get_pass(f: PassCB iso) =>
    // Debug.out("get_pass")
    got_pass("macflytest", consume f)

  be get_user(f: UserCB iso) =>
    f(_user)
     

actor Session
  let _env: Env
  let _pool: ConnectionPool

  new create(env: Env,
             host: String="",
             service: String="5432",
             user: String="",
             password: String = "",
             database: String = "") =>
    _env = env
    _pool = ConnectionPool(this, host, service, user,
      recover val [("user", user), ("database", database)] end)

  be log(msg: String) =>
    _env.out.print(msg)

  be connect() =>
    """Create a connection and try to log in"""
    try _pool.connect(_env.root as AmbientAuth) end

  be connected() => None

  be terminate()=> None

