use "debug"
use "crypto"
use "logger"

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
    _pool.get_user(recover {(u: String)(that, pass, req) => that._send_md5_pass(pass, u, req.salt)} end)

  be message(m: ServerMessage val!) =>
    let that = recover tag this end
    match m
    | let r: ClearTextPwdRequest val! =>
      _pool.get_pass(recover {(s: String)(that) => that.send_clear_pass(s)} end)
    | let r: MD5PwdRequest val  =>
      _pool.get_pass(recover {(s: String)(that, r) => that.send_md5_pass(s, r)} end)
    | let r: AuthenticationOkMessage val => None
    | let r: ReadyForQueryMessage val => _conn.next()
    else
      _conn.handle_message(m)
    end


class NullFetchNotify is FetchNotify
  fun ref batch(records: Array[Record val] val, next: FetchNotifyNext val) =>
    next(None)

actor Fetcher
  var _notify: FetchNotify iso = recover iso NullFetchNotify end
  let _conv: FetchConversation tag

  new create(conv: FetchConversation tag, n: FetchNotify iso) =>
    _notify = consume n
    _conv =  conv

  be apply(records: Array[Record val] val, next: FetchNotifyNext val) =>
    _notify.batch(records, next)

  be set_notifier(fn: (FetchNotify iso | None)) =>
    match consume fn
    | let f: FetchNotify iso => _notify = consume f
    end

  be stop() =>
    _notify.stop()
    


actor FetchConversation is Conversation
  let query: String val
  let params: Array[PGValue] val
  let _conn: BEConnection tag
  var _tuple_desc: (TupleDescription val | None) = None
  var _buffer: Array[Record val] trn = recover trn Array[Record val] end
  let _size: USize
  var _complete: Bool = false
  let logger: Logger[String val] val
  let fetcher: Fetcher tag

  new create(c: BEConnection tag, q: String,
             n: FetchNotify iso, p: Array[PGValue] val, out: OutStream) =>
    query = q
    params = p
    _conn = c
    _size = n.size()
    fetcher = Fetcher(this, consume n)
    logger = StringLogger(Warn, out)

  be _batch(b: BatchRowMessage val) =>
    Debug.out(query)
    try 
      for m in b.rows.values() do
        let record = recover val Record(_tuple_desc as TupleDescription val, m.fields) end
        _buffer.push(record)
        if (_buffer.size() == _size) and (_size > 0) then
          _do_send()
        end 
      end
    else
      Debug.out("can't create and push record")
    end

  be _next() =>
    Debug.out("next")
    if not _complete then _execute() else Debug.out("Nope") end

  be _send() =>
    logger(Fine) and logger.log("coucou")
    if _buffer.size() > 0 then
      _do_send()
    end

  be stop() =>
    fetcher.stop()

  fun ref _do_send() =>
    Debug.out("send")
    let b = _buffer = recover trn Array[Record val] end
    let that = recover tag this end
    fetcher(consume val b, recover val
      {(fn: (FetchNotify iso | None)=None) (that) => 
        fetcher.set_notifier(consume fn)
        that._next()}
    end)

  be message(m: ServerMessage val)=>
    match m
    | let r: ParseCompleteMessage val => None //_bind()
    | let r: CloseCompleteMessage val => _sync()
    | let r: BindCompleteMessage val => None //_describe()
    | let r: ReadyForQueryMessage val => _conn.next()
    | let r: BatchRowMessage val => _batch(r)
    | let r: RowDescriptionMessage val =>
      Debug.out("row_desc")
      _tuple_desc = r.tuple_desc
      _execute()
    | let r: EmptyQueryResponse val => Debug.out("Empty Query")
    | let r: CommandCompleteMessage val =>
      Debug.out("Completed")
      _complete = true
      _close()
      _send()
      stop()
    | let r: PortalSuspendedMessage val =>  _send()
    else
      _conn.handle_message(m)
    end

  be log(msg: String) => _conn.log(msg)

  be _sync() =>
    Debug.out("sync")
    _conn.writev(recover val SyncMessage.done() end)

  be _flush() =>
    Debug.out("flush")
    _conn.writev(recover val FlushMessage.done() end)

  be apply(c: BEConnection tag) =>
    Debug.out("apply")
    c.writev(recover val ParseMessage(query, "", TypeOids(params)).done() end)
    _bind()
    _describe()

  be _bind() =>
    Debug.out("bind")
    _conn.writev(recover val BindMessage("", "", params).done() end)

  be _execute() =>
    Debug.out("execute")
    _flush()
    _conn.writev(recover val ExecuteMessage("", _size).done() end)

  be _describe() =>
    Debug.out("describe")
    _flush()
    _conn.writev(recover val DescribeMessage('P', "").done() end)

  fun _close() =>
    Debug.out("close")
    _flush()
    _conn.writev(recover val CloseMessage('P', "").done() end)


actor ExecuteConversation is Conversation
  let query: String val
  let params: Array[PGValue] val
  let _conn: BEConnection tag
  let _handler: RecordCB val
  var _rows: Rows trn = recover trn Rows end
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
      _rows.push(res)
    end

  be call_back() =>
    let rows = _rows = recover trn Rows end
    _handler(consume val rows)

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
  var _rows: Rows trn = recover trn Rows end
  var _tuple_desc: (TupleDescription val | None) = None

  new create(c: BEConnection tag, q: String, h: RecordCB val) =>
    query = q
    _conn = c
    _handler = h

  be log(msg: String) => _conn.log(msg)

  be apply(c: BEConnection tag) =>
    c.writev(recover val QueryMessage(query).done() end)

  be call_back() =>
    let rows = _rows = recover trn Rows end
    _handler(consume val rows)

  be row(m: DataRowMessage val) =>
    try
      let res = recover val Record(_tuple_desc as TupleDescription val, m.fields) end
      _rows.push(res)
    end

  be batch(r: BatchRowMessage val) =>
    for row' in r.rows.values() do
      try
        let res = recover val Record(_tuple_desc as TupleDescription val, row'.fields) end
        _rows.push(res)
      end
    end

  be message(m: ServerMessage val) =>
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
