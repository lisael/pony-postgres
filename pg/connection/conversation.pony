use "debug"
use "crypto"

use "pg/protocol"
use "pg/codec"
use "pg/introspect"
use "pg"

trait Conversation
  
  be apply(c: BEConnection tag) 
  be message(m: ServerMessage val)


actor NullConversation is Conversation
  let _conn: BEConnection tag

  new create(c: BEConnection tag) => _conn = c
  be apply(c: BEConnection tag) => None
  be message(m: ServerMessage val) =>
    _conn.handle_message(m)


actor AuthConversation is Conversation
  let _pool: ConnectionManager
  let _params: Array[(String, String)] val
  let _conn: BEConnection tag
  new create(p: ConnectionManager, c: BEConnection tag, params: Array[(String, String)] val) =>
    _pool=p
    _conn=c
    _params=params

  be log(msg: String) =>
    _pool.log(msg)

  be apply(c: BEConnection tag) =>
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


trait FetchStatus

primitive _Sending is FetchStatus
primitive _Paused is FetchStatus
primitive _Suspended is FetchStatus

type Sending is _Sending val
type Paused is _Paused val
type Suspended is _Suspended val

actor RecordIterator
  let _notify: FetchNotify ref
  let _conversation: FetchConversation tag

  new create(n: FetchNotify iso, c: FetchConversation) =>
    _notify = consume n
    _conversation = c
    _conversation.start()

  be batch(b: Array[Record val] val) =>
    for r in b.values() do
      _notify.record(r)
    end
    _conversation.start()

  be record(r: Record val) =>
    _notify.record(r)
    _conversation.start()

  be stop() => _notify.stop()


actor FetchConversation is Conversation
  let query: String val
  let params: Array[PGValue] val
  let _conn: BEConnection tag
  var _rows: (Rows val | Rows trn ) = recover trn Rows end
  var _tuple_desc: (TupleDescription val | None) = None
  var _buffer: (Array[Record val] trn | Array[Record val] val)
  var status: FetchStatus val= Paused
  let _iterator: RecordIterator tag
  let _size: USize

  new create(c: BEConnection tag, q: String,
             n: FetchNotify iso, p: Array[PGValue] val) =>
    query = q
    params = p
    _conn = c
    let notify = consume n
    _size = notify.size()
    _buffer = recover trn Array[Record val] end
    _iterator = RecordIterator(consume notify, this)

  be start() => 
    if status is Suspended then _execute() end
    if _buffer.size() > 0 then
       /*Debug.out("Batch " + _buffer.size().string())*/
       _buffer = recover val _buffer end
       let b = _buffer = recover trn Array[Record val] end
       status = Paused
       _iterator.batch(b)
    end
    status = Sending

  be pause() =>
    /*Debug.out("paused")*/
    if status is Sending then status = Paused end

  be log(msg: String) => _conn.log(msg)

  fun _sync() =>
    _conn.writev(recover val SyncMessage.done() end)

  fun _flush() =>
    _conn.writev(recover val FlushMessage.done() end)

  be apply(c: BEConnection tag) =>
    c.writev(recover val ParseMessage(query, "", TypeOids(params)).done() end)
    _bind()
    _describe()
    _flush()

  be _bind() =>
    _conn.writev(recover val BindMessage("", "", params).done() end)
    _flush()

  be _execute() =>
    /*Debug.out("execute")*/
    _conn.writev(recover val ExecuteMessage("", _size).done() end)
    _flush()

  be _describe() =>
    _conn.writev(recover val DescribeMessage('P', "").done() end)
    _flush()

  be _close() =>
    _conn.writev(recover val CloseMessage('P', "").done() end)
    _flush()

  be row(m: DataRowMessage val) =>
    _handle_row(m)

  fun ref _handle_row(m: DataRowMessage val) =>
    /*Debug.out("row")*/
    try
      let record = recover val Record(_tuple_desc as TupleDescription val, m.fields) end
      if status is Sending then
        /*Debug.out("Sending")*/
        status = Paused
        _iterator.record(record) 
      else
        /*Debug("Buffer")*/
        try (_buffer as Array[Record val] trn).push(record) end
      end
    end

  be batch(rows: Array[DataRowMessage val] val) =>
    for row' in rows.values() do
      _handle_row(row')
    end


  be message(m: ServerMessage val)=>
    match m
    | let r: ParseCompleteMessage val => None //_bind()
    | let r: CloseCompleteMessage val => _sync()
    | let r: BindCompleteMessage val => None //_describe()
    | let r: ReadyForQueryMessage val => _conn.next()
    | let r: BatchRowMessage val =>
      for row' in r.rows.values() do
        row(row')
      end
      //batch(r.rows)
    | let r: RowDescriptionMessage val =>
      _tuple_desc = r.tuple_desc
      _execute()
    | let r: DataRowMessage val => row(r)
    | let r: EmptyQueryResponse val => Debug.out("Empty Query")
    | let r: CommandCompleteMessage val => _iterator.stop(); _close()
    | let r: PortalSuspendedMessage val => match status
      | Sending => _execute()
      | Paused => status = Suspended
      end
    else
      _conn.handle_message(m)
    end

actor ExecuteConversation is Conversation
  let query: String val
  let params: Array[PGValue] val
  let _conn: BEConnection tag
  let _handler: RecordCB val
  var _rows: (Rows val | Rows trn ) = recover trn Rows end
  var _tuple_desc: (TupleDescription val | None) = None

  new create(c: BEConnection tag, q: String, h: RecordCB val, p: Array[PGValue] val) =>
    query = q
    params = p
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
      let res = recover val Record(_tuple_desc as TupleDescription val, m.fields) end
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
  let _conn: BEConnection tag
  let _handler: RecordCB val
  var _rows: (Rows val | Rows trn ) = recover trn Rows end
  var _tuple_desc: (TupleDescription val | None) = None

  new create(c: BEConnection tag, q: String, h: RecordCB val) =>
    query = q
    _conn = c
    _handler = h

  be log(msg: String) => _conn.log(msg)

  be apply(c: BEConnection tag) =>
    c.writev(recover val QueryMessage(query).done() end)

  be call_back() =>
    // TODO; don't fail silently
    try
      _rows = recover val  _rows as Rows trn end
      _handler(_rows as Rows val)
    end

  be row(m: DataRowMessage val) =>
    try
      let res = recover val Record(_tuple_desc as TupleDescription val, m.fields) end
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

  be apply(c: BEConnection tag) =>
    c.writev(recover val TerminateMessage.done() end)

  be message(m: ServerMessage val)=>
    match m
    | let r: ConnectionClosedMessage val => _conn.do_terminate()
    else
      _conn.handle_message(m)
    end
