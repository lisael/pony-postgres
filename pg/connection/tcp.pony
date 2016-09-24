use "net"
use "collections"
use "debug"

use "pg/introspect"
use "pg/protocol"
use "pg"

interface BEConnection
  be raw(q: String, f: RowsCB val)

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
    /*_listener.received(recover [as U8: 0, 0, 0, 0, 0]end)*/
    _listener.terminate()
    _conn.received(ConnectionClosedMessage)

actor _Connection
  let _conn: TCPConnection tag
  var _fe: ( Connection tag | None) = None // front-end connection
  let _pool: ConnectionManager tag
  let _listener: Listener tag
  let _params: Array[(String, String)] val
  var _convs: List[_Conversation tag] = List[_Conversation tag]
  var _current: _Conversation tag
  var _backend_key: (U32, U32) = (0, 0)
  
  new create(auth: AmbientAuth,
             host: String,
             service: String,
             params: Array[(String, String)] val,
             pool: ConnectionManager) =>
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
    try (_fe as Connection).do_terminate() end

  be set_frontend(c: Connection tag) =>
    _fe = c



