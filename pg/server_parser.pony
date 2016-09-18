use "buffered"
use "debug"

trait ParseEvent
primitive ParsePending is ParseEvent
class ParseError is ParseEvent
  let msg: String

  new iso create(msg': String)=>
    msg = msg'



actor Listener
  let _conn: Connection
  var r: Reader iso = Reader // current reader
  var _ctype: U8 = 0 // current type (keep it if the data is chuncked)
  var _clen: USize = 0 // current message len (as given by server)

  new create(c: Connection) =>
    _conn = c

  be received(data: Array[U8] iso) =>
    let data' = recover val (consume data).slice() end
    r.append(data')
    while r.size() > _clen do
      match parse_response()
      | let result: ParseError val => _conn.log(result.msg)
      | let result: ErrorMessage val =>
        _conn.log("Error:")
        for (typ, value) in result.items.values() do
          _conn.log("  " + typ.string() + ": " + String.from_array(value))
        end
      | let result: ParsePending val => Debug.out("pending")
      | let result: ServerMessage val => Debug("sm");_conn.received(result)
      end
   end

  fun ref parse_type(): U8 ? =>
    if _ctype > 0 then return _ctype end
    _ctype = r.u8()

  fun ref parse_len(): USize ? =>
    if _clen > 0 then return _clen end
    _clen = r.i32_be().usize()

  fun ref parse_response(): (ServerMessage val|ParseEvent val) =>
    Debug.out("parse response:")
    Debug.out(" _ctype: " + _ctype.string())
    Debug.out(" _clen: " + _clen.string())
    try
      parse_type()
      parse_len()
    else
      Debug.out(" Pending"); return ParsePending
    end
    Debug.out(" parse len and type: ")
    Debug.out("  _ctype: " + _ctype.string())
    Debug.out("  _clen: " + _clen.string())
    if _clen > ( r.size() + 4) then
      Debug.out(" Pending (_clen: " + _clen.string()
        + ", r.size: " + r.size().string() + ")" )
      return ParsePending
    end
    let result = match _ctype
    | 69 => parse_err_resp()// E
    | 75 => parse_backend_key_data() //k
    | 82 => try  // R
        parse_auth_resp()
      else
        ParseError("Couldn't parse auth message")
      end
    | 83 => parse_parameter_status() // S
    | 90 => parse_ready_for_query() // Z
    else
      try r.block(_clen-4) else return ParseError("") end
      let ret = ParseError("Unknown message ID " + _ctype.string())
      _ctype = 0
      _clen = 0
      ret
    end
    match result
    | let res: ServerMessage val =>
      _ctype = 0
      _clen = 0
    end
    result

  fun ref parse_backend_key_data(): ServerMessage val =>
    try
      let pid = r.u32_be()
      let key = r.u32_be()
      BackendKeyDataMessage(pid, key)
    else
      ParseError("Unreachable")
    end

  fun ref parse_ready_for_query(): ServerMessage val =>
    let b = try r.u8() else return ParseError("Unreachable") end
    ReadyForQueryMessage(b)

  fun ref parse_parameter_status(): ServerMessage val =>
    let item = try
        recover val r.block(_clen-4).slice() end
      else
        return ParseError("This should never happen")
      end
    Debug.out(String.from_array(item))
    let end_idx = try item.find(0) else return ParseError("Malformed parameter message") end
    ParameterStatusMessage(
      recover val item.trim(0, end_idx) end,
      recover val item.trim(end_idx + 1) end)

  fun ref parse_auth_resp(): ServerMessage val ?=>
    Debug.out("parse_auth_resp")
    let msg_type = r.i32_be()
    /*Debug.out(msg_type)*/
    let result: ServerMessage val = match msg_type // auth message type
    | 0 => AuthenticationOkMessage
    | 3 => ClearTextPwdRequest
    | 5 => MD5PwdRequest(recover val [r.u8(), r.u8(), r.u8(), r.u8()] end)
    else 
      ParseError("Unknown auth message")
    end
    result

  fun ref parse_err_resp(): ServerMessage val =>
    // TODO: This is ugly. it used to work with other
    // capabilities, so I adapted to get a val fields. It copies 
    // all, it should not.
    Debug.out("parse_err_resp")
    let it = recover val
      let items = Array[(U8, Array[U8] val)]
      let fields' = try r.block(_clen - 4) else
        return ParseError("")
      end
      let fields = recover val (consume fields').slice() end
      var pos: USize = 1
      var start_pos = pos
      let iter = fields.values()
      var c = try iter.next() else return ParseError("Bad error format") end
      var typ = c
      repeat
        //Debug.out(c)
        /*Debug.out("#" + pos.string())*/
        if c == 0 then
          //Debug.out("*" + typ.string())
          if typ == 0 then break
          else
            items.push((typ, fields.trim(start_pos, pos)))
            start_pos = pos + 1
            typ = 0
          end
        else 
          if typ == 0 then typ = c end
        end
        c = try iter.next() else if typ == 0 then break else 0 end end
        pos = pos + 1
      until false end
      items
    end
    ErrorMessage(it)

    
