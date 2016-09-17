use "buffered"
use "debug"

class ParseError is ServerMessage
  let msg: String

  new iso create(msg': String)=>
    msg = msg'

primitive ParseResponse
  fun apply(s: Array[U8] iso): ServerMessage val =>
    ResponseParser(consume s)()

actor Listener
  let _conn: Connection

  new create(c: Connection) =>
    _conn = c

  be received(data: Array[U8] iso) =>
    match ParseResponse(consume data)
    | let r: ParseError val => _conn.log(r.msg)
    | let r: ErrorMessage val =>
      _conn.log("Error:")
      for (typ, value) in r.items.values() do
        _conn.log("  " + typ.string() + ": " + String.from_array(value))
      end
    | let r: ServerMessage val => r
    end

class ResponseParser
  let r: Reader = Reader
  let src: Array[U8] val
  
  new create(s: Array[U8] val) =>
    src=s
    r.append(s)

  fun ref apply(): ServerMessage val =>
    let id = try r.u8() else return ParseError("Empty message") end
    Debug.out(id)
    match id
    | 82 => return try  // R
        parse_auth_resp()
      else
        ParseError("Couldn't parse auth message")
      end
    | 69 => return try  // E
        parse_err_resp()
      else
        ParseError("Couldn't parse error message")
      end
    else
      return ParseError("Unknown message ID " + id.string())
    end

  fun ref parse_err_resp(): ServerMessage val ?=>
    r.i32_be().usize() // msg length TODO: check the actual length
    let it = recover val
      let items = Array[(U8, Array[U8] val)]
      let fields =  src.trim(5)
      var pos: USize = 1
      var start_pos = pos
      let iter = fields.values()
      var c = try iter.next() else return ParseError("Bad error format") end
      var typ = c
      repeat
        Debug.out(c)
        /*Debug.out("#" + pos.string())*/
        if c == 0 then
          Debug.out("*" + typ.string())
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

  fun ref parse_auth_resp(): ServerMessage val ?=>
    r.i32_be() // msg length TODO: check the actual length
    let msg_type = r.i32_be()
    Debug.out(msg_type)
    let result: ServerMessage val = match msg_type // auth message type
    | 0 => AuthenticationOkMessage
    | 3 => ClearTextPwdRequest
    | 5 => MD5PwdRequest
    else 
      ParseError("Unknown auth message")
    end
    result
    
