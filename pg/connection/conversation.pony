use "debug"
use "crypto"

use "pg/protocol"
use "pg/codec"
use "pg/introspect"
use "pg"

trait Conversation
  
  be apply(c: _Connection) 
  be message(m: ServerMessage val)


actor _NullConversation is Conversation
  let _conn: _Connection

  new create(c: _Connection) => _conn = c
  be apply(c: _Connection) => None
  be message(m: ServerMessage val) =>
    _conn.handle_message(m)


actor _AuthConversation is Conversation
  let _pool: ConnectionManager
  let _params: Array[(String, String)] val
  let _conn: _Connection
  new create(p: ConnectionManager, c: _Connection, params: Array[(String, String)] val) =>
    _pool=p
    _conn=c
    _params=params

  be log(msg: String) =>
    _pool.log(msg)

  be apply(c: _Connection) =>
    let data = recover val
    let msg = StartupMessage(_params)
    msg.done() 
    end
    c.writev(data)

  be send_clear_pass(pass: String) =>
    _conn.writev(recover val PasswordMessage(pass).done() end)

  be _send_md5_pass(pass: String, username: String, salt: Array[U8] val) =>
    // TODO: Make it work. doesn't work at the moment
    // from PG doc : concat('md5', md5(concat(md5(concat(password, username)), random-salt)))
    var result = "md5" + ToHexString(
      MD5(
        ToHexString(MD5(pass+username)) + String.from_array(salt)
      )
    )
    // Debug(recover val ToHexString(MD5(pass+username)) + String.from_array(salt') end)
    // Debug(result)
    _conn.writev(recover val PasswordMessage(result).done() end)

  be send_md5_pass(pass: String, req: MD5PwdRequest val) =>
    Debug.out(pass)
    let that = recover tag this end
    _pool.get_user(recover lambda(u: String)(that, pass, req) => that._send_md5_pass(pass, u, req.salt) end end)

  be message(m: ServerMessage val!) =>
    let that = recover tag this end
    match m
    | let r: ClearTextPwdRequest val! =>
      _pool.get_pass(recover lambda(s: String)(that) => that.send_clear_pass(s) end end)
    | let r: MD5PwdRequest val  =>
      _pool.get_pass(recover lambda(s: String)(that, r) => that.send_md5_pass(s, r) end end)
    | let r: AuthenticationOkMessage val => None
    | let r: ReadyForQueryMessage val => _conn.next()
    else
      _conn.handle_message(m)
    end

actor ExecuteConversation is Conversation
  let query: String val
  let params: Array[PGValue] val
  /*let param_types: Array[I32]*/
  let _conn: BEConnection tag
  let _handler: ResultCB val
  var _rows: (Rows val | Rows trn ) = recover trn Rows end
  var _tuple_desc: (TupleDescription val | None) = None

  new create(c: BEConnection tag, q: String, h: ResultCB val, p: Array[PGValue] val) =>
    query = q
    params = p
    /*param_types = */
    _conn = c
    _handler = h

  be log(msg: String) => _conn.log(msg)

  fun _sync() =>
    _conn.writev(recover val SyncMessage.done() end)

  fun _flush() =>
    _conn.writev(recover val FlushMessage.done() end)

  be apply(c: BEConnection tag) =>
    c.writev(recover val ParseMessage(query, "", TypeOids(params)).done() end)
    _flush()

  be _bind() =>
    _conn.writev(recover val BindMessage("", "", params).done() end)
    _flush()

  be _execute() =>
    _conn.writev(recover val ExecuteMessage("", 0).done() end)
    _flush()

  be _describe() =>
    _conn.writev(recover val DescribeMessage('P', "").done() end)
    _flush()

  be _close() =>
    _conn.writev(recover val CloseMessage('P', "").done() end)
    _flush()

  be row(m: DataRowMessage val) =>
    try
      let res = recover val Result(_tuple_desc as TupleDescription val, m.fields) end
      (_rows as Rows trn).push(res)
    end


  be call_back() =>
    // TODO; don't fail silently
    try
      _rows = recover val  _rows as Rows trn end
      _handler(_rows as Rows val)
    end

  be message(m: ServerMessage val)=>
    match m
    | let r: ParseCompleteMessage val => _bind()
    | let r: CloseCompleteMessage val => _sync()
    | let r: BindCompleteMessage val => _describe()
    | let r: ReadyForQueryMessage val => _conn.next()
    | let r: RowDescriptionMessage val =>
      _tuple_desc = r.tuple_desc
      _execute()
    | let r: DataRowMessage val => row(r)
    | let r: EmptyQueryResponse val => Debug.out("Empty Query")
    | let r: CommandCompleteMessage val => call_back(); _close()
    else
      _conn.handle_message(m)
    end

actor QueryConversation is Conversation
  let query: String val
  let _conn: _Connection
  let _handler: ResultCB val
  var _rows: (Rows val | Rows trn ) = recover trn Rows end
  var _tuple_desc: (TupleDescription val | None) = None

  new create(c: _Connection, q: String, h: ResultCB val) =>
    query = q
    _conn = c
    _handler = h

  be log(msg: String) => _conn.log(msg)

  be apply(c: _Connection) =>
    c.writev(recover val QueryMessage(query).done() end)

  be call_back() =>
    // TODO; don't fail silently
    try
      _rows = recover val  _rows as Rows trn end
      _handler(_rows as Rows val)
    end

  be row(m: DataRowMessage val) =>
    try
      let res = recover val Result(_tuple_desc as TupleDescription val, m.fields) end
      (_rows as Rows trn).push(res)
    end

  be message(m: ServerMessage val)=>
    match m
    | let r: EmptyQueryResponse val => Debug.out("Empty Query")
    | let r: CommandCompleteMessage val => call_back(); Debug.out(r.command)
    | let r: ReadyForQueryMessage val => _conn.next()
    | let r: RowDescriptionMessage val => _tuple_desc = r.tuple_desc
    | let r: DataRowMessage val => row(r)
    else
      _conn.handle_message(m)
    end

actor TerminateConversation is Conversation
  let _conn: BEConnection tag

  new create(c: BEConnection tag) =>
    _conn = c

  be log(msg: String) => _conn.log(msg)

  be apply(c: _Connection) =>
    c.writev(recover val TerminateMessage.done() end)

  be message(m: ServerMessage val)=>
    match m
    | let r: ConnectionClosedMessage val => _conn.do_terminate()
    else
      _conn.handle_message(m)
    end
