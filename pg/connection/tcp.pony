use "net"
use "collections"
use "debug"

use "pg/introspect"
use "pg/protocol"
use "pg/codec"
use "pg"

interface BEConnection
  be raw(q: String, f: RowsCB val)
  be execute(query: String, params: Array[PGValue] val, handler: RowsCB val)
  be writev(data: ByteSeqIter)
  be log(msg: String)
  be handle_message(s: ServerMessage val)
  be next()
  be schedule(conv: Conversation tag)
  be do_terminate()

class PGNotify is TCPConnectionNotify
  let _conn: _Connection
  let _listener: Listener

  new iso create(c: _Connection, l: Listener) =>
    _conn = c
    _listener = l

  fun ref connected(conn: TCPConnection ref) =>
    _conn.connected()

  fun ref received(conn: TCPConnection ref, data: Array[U8] iso) =>
    // Debug.out("received")
    _listener.received(consume data)

  fun ref closed(conn: TCPConnection ref) =>
    /*_listener.received(recover [as U8: 0, 0, 0, 0, 0]end)*/
    _listener.terminate()
    _conn.received(ConnectionClosedMessage)

  /*fun ref sent(conn: TCPConnection ref, data: (String val | Array[U8 val] val)): (String val | Array[U8 val] val) =>*/
    /*Debug.out("send")*/
    /*match data*/
    /*| let s: String val => for c in s.values() do Debug.out(c) end*/
    /*| let s: Array[U8 val] val => for c in s.values() do Debug.out(c) end*/
    /*end*/
    /*conn.write_final(data)*/
    /*""*/

actor _Connection is BEConnection
  let _conn: TCPConnection tag
  var _fe: ( Connection tag | None) = None // front-end connection
  let _pool: ConnectionManager tag
  let _listener: Listener tag
  let _params: Array[(String, String)] val
  var _convs: List[Conversation tag] = List[Conversation tag]
  var _current: Conversation tag
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


  fun ref _schedule(conv: Conversation tag) =>
    match _current
    | let n: _NullConversation =>
      _current = conv
      _current(this)
    else
      _convs.push(conv)
    end

  be execute(query: String, params: Array[PGValue] val, handler: RowsCB val) =>
    schedule(ExecuteConversation(this, query, params, handler))

  be raw(q: String, handler: RowsCB val) =>
    schedule(_QueryConversation(q, this, handler))

  be schedule(conv: Conversation tag) =>
    _schedule(conv)

  be connected() =>
    _current(this)

  be _set_backend_key(m: BackendKeyDataMessage val) =>
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
    None

  be received(s: ServerMessage val) =>
    // Debug.out("recieved " + s.string())
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

  be terminate() =>
    schedule(_TerminateConversation(this))

  be do_terminate() =>
    try (_fe as Connection).do_terminate() end

  be set_frontend(c: Connection tag) =>
    _fe = c



