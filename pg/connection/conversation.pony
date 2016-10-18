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

actor FetchConversation is Conversation
  let query: String val
  let params: Array[PGValue] val
  let _conn: BEConnection tag
  var _rows: (Rows val | Rows trn ) = recover trn Rows end
  var _tuple_desc: (TupleDescription val | None) = None
  var _buffer: (Array[Record val] trn | Array[Record val] val)
  let _buffers: Array[Array[Record val] val] = Array[Array[Record val] val]
  var _notify: (FetchNotify iso | None) 
  let _size: USize

  new create(c: BEConnection tag, q: String,
             n: FetchNotify iso, p: Array[PGValue] val) =>
    query = q
    params = p
    _conn = c
    _size = n.size()
    _notify = consume n
    _buffer = recover trn Array[Record val] end

  be _batch(b: BatchRowMessage val) =>
    Debug.out(query)
    try 
      for m in b.rows.values() do
        let record = recover val Record(_tuple_desc as TupleDescription val, m.fields) end
        (_buffer as Array[Record val] trn).push(record)
        if _buffer.size() == _size then
          _do_send()
        end 
      end
    else
      Debug.out("can't create and push record")
    end

  be _set_notifier(fn: (FetchNotify iso | None)) =>
    match consume fn
    | let f: FetchNotify iso => _notify = consume f
    end

  be _next() =>
    _execute()

  be _stop() =>
    try (_notify as FetchNotify iso).stop() end
    
  be _send() =>
    _do_send()

  fun ref _do_send() =>
   Debug.out("send")
    _buffer = recover val _buffer end
    let b = _buffer = recover trn Array[Record val] end
    try
      let that = recover tag this end
      (_notify as FetchNotify iso).batch(b, recover val
        lambda(fn: (FetchNotify iso | None)=None) (that) => 
          that._set_notifier(consume fn)
          that._next()
        end
      end)
    else
      _buffers.push(b)
    end

  be message(m: ServerMessage val)=>
    match m
    | let r: ParseCompleteMessage val => None //_bind()
    | let r: CloseCompleteMessage val => _sync()
    | let r: BindCompleteMessage val => None //_describe()
    | let r: ReadyForQueryMessage val => _conn.next()
    | let r: BatchRowMessage val =>
      None
      //for row' in r.rows.values() do
      //  row(row')
      //end
      _batch(r)
    | let r: RowDescriptionMessage val =>
      _tuple_desc = r.tuple_desc
      _execute()
    | let r: EmptyQueryResponse val => Debug.out("Empty Query")
    | let r: CommandCompleteMessage val =>
      _send()
      _stop()
      _close()
    | let r: PortalSuspendedMessage val =>  None
    else
      _conn.handle_message(m)
    end

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

actor FetchConversation2 is Conversation
  let query: String val
  let params: Array[PGValue] val
  let _conn: BEConnection tag
  var _rows: (Rows val | Rows trn ) = recover trn Rows end
  var _tuple_desc: (TupleDescription val | None) = None
  var _buffer: (Array[Record val] trn | Array[Record val] val)
  var status: FetchStatus val= Suspended
  let _notify: FetchNotify iso
  let _size: USize

  new create(c: BEConnection tag, q: String,
             n: FetchNotify iso, p: Array[PGValue] val) =>
    query = q
    params = p
    _conn = c
    _size = n.size()
    _notify = consume n
    _buffer = recover trn Array[Record val] end

  be start() => 
    if status is Suspended then _execute() end
    if _buffer.size() > 0 then
       /*Debug.out("Batch " + _buffer.size().string())*/
       _buffer = recover val _buffer end
       let b = _buffer = recover trn Array[Record val] end
       status = Paused
       //_notify.batch(b)
    end
    status = Sending

  be pause() =>
    /*Debug.out("paused")*/
    if status is Sending then status = Paused end

  be row(m: DataRowMessage val) =>
    Debug.out("row")
    _handle_row(m)

  fun ref _handle_row(m: DataRowMessage val) =>
    try
      let record = recover val Record(_tuple_desc as TupleDescription val, m.fields) end
      if status is Sending then
        Debug.out("Sending")
        status = Paused
        _notify.record(record) 
        start()
      else
        Debug("Buffer")
        (_buffer as Array[Record val] trn).push(record)
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
      None
      //for row' in r.rows.values() do
      //  row(row')
      //end
      batch(r.rows)
    | let r: RowDescriptionMessage val =>
      _tuple_desc = r.tuple_desc
      start()
    | let r: EmptyQueryResponse val => Debug.out("Empty Query")
    | let r: CommandCompleteMessage val => _close()
    | let r: PortalSuspendedMessage val => match status
      | Sending => Debug.out("continue");_execute()
      | Paused => Debug.out("suspend");status = Suspended
      end
    else
      _conn.handle_message(m)
    end

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
    | let r: BatchRowMessage val =>
      for row' in r.rows.values() do
        row(row')
      end
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
    Debug.out("coucou")
    try
      _rows = recover val  _rows as Rows trn end
      _handler(_rows as Rows val)
    end

  be row(m: DataRowMessage val) =>
    try
      let res = recover val Record(_tuple_desc as TupleDescription val, m.fields) end
      (_rows as Rows trn).push(res)
      Debug.out(res(0))
    end

  be batch(r: BatchRowMessage val) =>
    for row' in r.rows.values() do
      try
        let res = recover val Record(_tuple_desc as TupleDescription val, row'.fields) end
        (_rows as Rows trn).push(res)
        Debug.out(res(0))
      end
    end

  be message(m: ServerMessage val)=>
    match m
    | let r: EmptyQueryResponse val => Debug.out("Empty Query")
    | let r: CommandCompleteMessage val => call_back(); Debug.out(r.command)
    | let r: ReadyForQueryMessage val => _conn.next()
    | let r: RowDescriptionMessage val => _tuple_desc = r.tuple_desc
    | let r: BatchRowMessage val =>
      Debug.out("Batch: " + r.rows.size().string() + " rows")
      batch(r)
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
