use "debug"
use "crypto"

use "pg/protocol"
use "pg/codec"
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
  let _handler: RowsCB val
  var _rows: (Rows | None) = None

  new create(c: BEConnection tag, q: String, p: Array[PGValue] val, h: RowsCB val) =>
    query = q
    params = p
    /*param_types = */
    _conn = c
    _handler = h

  be log(msg: String) => _conn.log(msg)

  fun flush() =>
    _conn.writev(recover val FlushMessage.done() end)

  be apply(c: BEConnection tag) =>
    Debug.out("#####")
    c.writev(recover val ParseMessage(query, "", recover [as I32: 23, 23] end).done() end)
    flush()

  be _bind() =>
    _conn.writev(recover val BindMessage("", "", params).done() end)
    flush()

  be _execute() =>
    _conn.writev(recover val ExecuteMessage("", 0).done() end)
    flush()

  be _describe() =>
    _conn.writev(recover val DescribeMessage('P', "").done() end)
    flush()

  be row(m: DataRowMessage val) =>
    try (_rows as Rows).append(m.fields) end

  be call_back() =>
    // TODO; don't fail silently
    Debug.out("call_back")
    try _handler(_rows as Rows) end

  be message(m: ServerMessage val)=>
    match m
    | let r: ParseCompleteMessage val => _bind()
    | let r: BindCompleteMessage val => _describe()
    | let r: ReadyForQueryMessage val => _conn.next()
    | let r: RowDescriptionMessage val => _rows = Rows(r.row); _execute()
    | let r: DataRowMessage val => row(r)
    | let r: EmptyQueryResponse val => Debug.out("Empty Query")
    | let r: CommandCompleteMessage val => call_back(); Debug.out(r.command)
    else
      _conn.handle_message(m)
    end

actor _QueryConversation is Conversation
  let query: String val
  let _conn: _Connection
  let _handler: RowsCB val
  var _rows: (Rows | None) = None

  new create(q: String, c: _Connection, h: RowsCB val) =>
    query = q
    _conn = c
    _handler = h

  be log(msg: String) => _conn.log(msg)

  be apply(c: _Connection) =>
    c.writev(recover val QueryMessage(query).done() end)

  be call_back() =>
    // TODO; don't fail silently
    try _handler(_rows as Rows) end

  be row(m: DataRowMessage val) =>
    try (_rows as Rows).append(m.fields) end

  be message(m: ServerMessage val)=>
    match m
    | let r: EmptyQueryResponse val => Debug.out("Empty Query")
    | let r: CommandCompleteMessage val => call_back(); Debug.out(r.command)
    | let r: ReadyForQueryMessage val => _conn.next()
    | let r: RowDescriptionMessage val => _rows = Rows(r.row)
    | let r: DataRowMessage val => row(r)
    else
      _conn.handle_message(m)
    end

actor _TerminateConversation is Conversation
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
