use "debug"
use "crypto"

use "pg/protocol"

trait _Conversation
  
  be apply(c: _Connection) 
  be message(m: ServerMessage val)

actor _NullConversation is _Conversation
  let _conn: _Connection

  new create(c: _Connection) => _conn = c
  be apply(c: _Connection) => None
  be message(m: ServerMessage val) =>
    _conn.handle_message(m)


actor _AuthConversation is _Conversation
  let _pool: _ConnectionPool
  let _params: Array[Param] val
  let _conn: _Connection
  new create(p: _ConnectionPool, c: _Connection, params: Array[Param] val) =>
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

  be send_md5_pass(pass: String, username: String, salt: Array[U8] val) =>
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

  be got_md5_pass(pass: String, req: MD5PwdRequest val) =>
    Debug.out(pass)
    let that = recover tag this end
    _pool.get_user(recover lambda(u: String)(that, pass, req) => that.send_md5_pass(pass, u, req.salt) end end)

  be message(m: ServerMessage val!) =>
    let that = recover tag this end
    match m
    | let r: ClearTextPwdRequest val! =>
      _pool.get_pass(recover lambda(s: String)(that) => that.send_clear_pass(s) end end)
    | let r: MD5PwdRequest val  =>
      _pool.get_pass(recover lambda(s: String)(that, r) => that.got_md5_pass(s, r) end end)
    | let r: AuthenticationOkMessage val => None
    | let r: ReadyForQueryMessage val => _conn.next()
    else
      _conn.handle_message(m)
    end

actor _QueryConversation is _Conversation
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

actor _TerminateConversation is _Conversation
  let _conn: _Connection

  new create(c: _Connection) =>
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
