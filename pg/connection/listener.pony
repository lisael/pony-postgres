use "buffered"
use "collections"
use "debug"

use "pg/protocol"
use "pg/introspect"

trait ParseEvent
  fun string(): String => "Unknown"
primitive ParsePending is ParseEvent
class PGParseError is ParseEvent
  let msg: String

  new iso create(msg': String)=>
    msg = msg'


actor Listener
  let _conn: BEConnection tag
  var r: Reader iso = Reader // current reader
  var _ctype: U8 = 0 // current type (keep it if the data is chuncked)
  var _clen: USize = 0 // current message len (as given by server)

  new create(c: BEConnection tag) =>
    _conn = c

  be received(data: Array[U8] iso) =>
    let data' = recover val (consume data).slice() end
    r.append(data')
    while r.size() > _clen do
      match parse_response()
      | let result: PGParseError val => _conn.log(result.msg)
      | let result: ParsePending val => return
      | let result: ServerMessage val => _conn.received(result)
      end
   end

  be terminate() =>
    if _ctype == 'E' then
      r.append(recover Array[U8].init(0, _clen - r.size()) end)
      try _conn.received(recover val parse_response() end as ServerMessage val) end
    end

  fun ref parse_type() ? =>
    if _ctype > 0 then return end
    _ctype = r.u8()

  fun ref parse_len() ? =>
    if _clen > 0 then return end
    _clen = r.i32_be().usize()

  fun ref parse_response(): (ServerMessage val|ParseEvent val) =>
    try
      parse_type()
      parse_len()
    else
      /*Debug.out("  Pending")*/
      return ParsePending
    end
    /*Debug.out("  _ctype: " + _ctype.string())*/
    /*Debug.out("  _clen: " + _clen.string())*/
    if _clen > ( r.size() + 4) then
      /*Debug.out("  Pending (_clen: " + _clen.string() + ", r.size: " + r.size().string() + ")" )*/
      return ParsePending
    end
    let result = match _ctype
    | '1' => ParseCompleteMessage
    | '2' => BindCompleteMessage
    | '3' => CloseCompleteMessage
    | 'C' => try
        CommandCompleteMessage(parse_single_string())
      else
        PGParseError("Couldn't parse cmd complete message")
      end
    | 'D' => parse_data_row()
    | 'E' => parse_err_resp()
    | 'I' => EmptyQueryResponse
    | 'K' => parse_backend_key_data()
    | 'R' => parse_auth_resp()
    | 'S' => parse_parameter_status()
    | 'T' => parse_row_description()
    | 'Z' => parse_ready_for_query()
    else
      try r.block(_clen-4) else return PGParseError("") end
      let ret = PGParseError("Unknown message ID " + _ctype.string())
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

  fun ref parse_data_row(): ServerMessage val =>
    try
      let n_fields = r.u16_be()
      let f = recover val
        let fields: Array[FieldData val]= Array[FieldData val](n_fields.usize())
        for n in Range(0, n_fields.usize()) do
          let len = r.i32_be()
          let data = recover val r.block(len.usize()) end
          fields.push(recover val FieldData(len, data) end)
        end
        fields
        end
      DataRowMessage(f)
    else
      PGParseError("Unreachable")
    end

  fun ref parse_string(): String val ? =>
    recover val
      let s = String
      while true do
        let c = r.u8()
        if c == 0 then break else s.push(c) end
      end
      s
    end

  fun ref parse_row_description(): ServerMessage val =>
    let rd = RowDescription
    try
      let n_fields = r.u16_be().usize()
      let field_descs = recover Array[FieldDescription val](n_fields) end
      for n in Range(0, n_fields) do
        let name = parse_string()
        let table_oid = r.i32_be()
        let col_number = r.i16_be()
        let type_oid = r.i32_be()
        let type_size = r.i16_be()
        let type_modifier = r.i32_be()
        let format = r.i16_be()
        let fd = recover val FieldDescription(name, table_oid, col_number,
                                   type_oid, type_size,
                                   type_modifier, format)end
        rd.append(fd)
        field_descs.push(fd)
      end
      let td = recover val TupleDescription(recover val consume field_descs end) end
      RowDescriptionMessage(recover val consume ref rd end, td)
    else
      PGParseError("Unreachable")
    end

  fun ref parse_single_string(): String ? =>
   String.from_array(r.block(_clen - 4))

  fun ref parse_backend_key_data(): ServerMessage val =>
    try
      let pid = r.u32_be()
      let key = r.u32_be()
      BackendKeyDataMessage(pid, key)
    else
      PGParseError("Unreachable")
    end

  fun ref parse_ready_for_query(): ServerMessage val =>
    let b = try r.u8() else return PGParseError("Unreachable") end
    ReadyForQueryMessage(b)

  fun ref parse_parameter_status(): ServerMessage val =>
    let item = try
        recover val r.block(_clen-4).slice() end
      else
        return PGParseError("This should never happen")
      end
    let end_idx = try
        item.find(0)
      else
        return PGParseError("Malformed parameter message")
      end
    ParameterStatusMessage(
      recover val item.trim(0, end_idx) end,
      recover val item.trim(end_idx + 1) end)

  fun ref parse_auth_resp(): ServerMessage val =>
    /*Debug.out("parse_auth_resp")*/
    try
      let msg_type = r.i32_be()
      /*Debug.out(msg_type)*/
      let result: ServerMessage val = match msg_type // auth message type
      | 0 => AuthenticationOkMessage
      | 3 => ClearTextPwdRequest
      | 5 => MD5PwdRequest(recover val [r.u8(), r.u8(), r.u8(), r.u8()] end)
      else 
        PGParseError("Unknown auth message")
      end
      result
    else
      PGParseError("Unreachable")
    end

  fun ref parse_err_resp(): ServerMessage val =>
    // TODO: This is ugly. it used to work with other
    // capabilities, so I adapted to get a val fields. It copies 
    // all, it should not.
    let it = recover val
      let items = Array[(U8, Array[U8] val)]
      let fields' = try r.block(_clen - 4) else
        return PGParseError("")
      end
      let fields = recover val (consume fields').slice() end
      var pos: USize = 1
      var start_pos = pos
      let iter = fields.values()
      var c = try iter.next() else return PGParseError("Bad error format") end
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

    
