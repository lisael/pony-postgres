use "net"
use "collections"
use "debug"
use "logger" 

use "pg/introspect"
use "pg/protocol"
use "pg/codec"
use "pg"

interface BEConnection
  be execute(query: String,
             handler: RecordCB val,
             params: (Array[PGValue] val | None) = None)
  be writev(data: ByteSeqIter)
  be log(msg: String)
  be handle_message(s: ServerMessage val)
  be next()
  be schedule(conv: Conversation tag)
  be terminate()
  be received(s: ServerMessage val)
  be do_terminate()
  be fetch(query: String, notify: FetchNotify iso,
           params: (Array[PGValue] val | None) = None)


actor _Connection is BEConnection
  let _conn: TCPConnection tag
  var _fe: ( Connection tag | None) = None // front-end connection
  let _pool: ConnectionManager tag
  let _params: Array[(String, String)] val
  var _convs: List[Conversation tag] = List[Conversation tag]
  var _current: Conversation tag
  var _backend_key: (U32, U32) = (0, 0)
  let out: OutStream
  let logger: Logger[String val] val
  
  new create(auth: AmbientAuth,
             host: String,
             service: String,
             params: Array[(String, String)] val,
             pool: ConnectionManager,
             out': OutStream
             ) =>
    _conn = TCPConnection(auth, PGNotify(this), host, service)
    _pool = pool
    _params = params
    _current = AuthConversation(_pool, this, _params)
    out = out'
    logger = StringLogger(Warn, out)

  be writev(data: ByteSeqIter) =>
    _conn.writev(data)

  fun ref _schedule(conv: Conversation tag) =>
    match _current
    | let n: NullConversation =>
      _current = conv
      _current(this)
    else
      _convs.push(conv)
    end

  be execute(query: String,
             handler: RecordCB val,
             params: (Array[PGValue] val | None) = None) =>
    match params
    | let p: None =>
      schedule(QueryConversation(this, query, handler))
    | let p: Array[PGValue] val =>
      schedule(ExecuteConversation(this, query, handler, p))
    end

  be fetch(query: String, notify: FetchNotify iso,
           params: (Array[PGValue] val | None) = None) =>
     schedule(
       FetchConversation(this, query, consume notify,
         try
           params as Array[PGValue] val
         else
           recover val Array[PGValue] end
         end, out
       )
     )

  be schedule(conv: Conversation tag) =>
    _schedule(conv)

  be connected() =>
    _current(this)

  be _set_backend_key(m: BackendKeyDataMessage val) =>
    _backend_key = m.data

  be log(msg: String) => 
    Debug.out(msg)
    _pool.log(msg)

  be next() =>
    try
      _current = _convs.shift()
      _current(this)
    else
      _current = NullConversation(this)
    end

  be update_param(p: ParameterStatusMessage val) =>
    // TODO: update the parameters and allow the user to query them
    None

  be received(s: ServerMessage val) =>
    logger(Fine) and logger.log("recieved " + s.string())
    _current.message(s)

  be _log_error(m: ErrorMessage val) =>
    for (tagg, text) in m.items.values() do
      let s: String trn = recover trn String(text.size() + 3) end
      s.push(tagg)
      s.append(": ")
      s.append(text)
      Debug.out(consume s)
    end

  be handle_message(s: ServerMessage val) =>
    match s
    | let m: ParameterStatusMessage val => update_param(m)
    | let m: BackendKeyDataMessage val => _set_backend_key(m)
    | let m: ErrorMessage val => _log_error(m)
    | let m: ConnectionClosedMessage val => log("Disconected")
    else
      log("Unknown ServerMessage: " + s.string())
    end

  be terminate() =>
    schedule(TerminateConversation(this))

  be do_terminate() =>
    try (_fe as Connection).do_terminate() end

  be set_frontend(c: Connection tag) =>
    _fe = c
