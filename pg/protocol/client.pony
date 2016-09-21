use "buffered"

type _Param is (String, String)

interface ClientMessage is Message
  fun ref _zero()
  fun ref _write(s: String) 
  fun ref _i32(i: I32)
  fun ref _done(id: U8): Array[ByteSeq] iso^
  fun ref done(): Array[ByteSeq] iso^ => _done(0)

class ClientMessageBase is ClientMessage
  var _w: Writer = Writer
  var _out: Writer = Writer

  fun ref _zero() => _w.u8(0)
  fun ref _u8(u: U8) => _w.u8(u)
  fun ref _write(s: String) => _w.write(s)
  fun ref _i32(i: I32) => _w.i32_be(i)
  fun ref _done(id: U8): Array[ByteSeq] iso^ =>
    if id != 0 then _out.u8(id) end
    _out.i32_be(_w.size().i32() + 4)
    _out.writev(_w.done())
    _out.done()

class NullClientMessage is ClientMessage
  fun ref _zero() => None
  fun ref _write(s: String)  => None
  fun ref _i32(i: I32) => None
  fun ref _done(id: U8): Array[ByteSeq] iso^ => recover Array[ByteSeq] end

class StartupMessage is ClientMessage
  let _base: ClientMessageBase delegate ClientMessage = ClientMessageBase

  new create(params: Array[_Param] box) =>
    _i32(196608) // protocol version 3.0
    for (key, value) in params.values() do
      add_param(key, value)
    end

  fun ref done(): Array[ByteSeq] iso^ => _zero(); _done(0)

  fun ref add_param(key: String, value: String) =>
    _write(key); _zero()
    _write(value); _zero()

class TerminateMessage
  let _base: ClientMessageBase delegate ClientMessage = ClientMessageBase
  fun ref done(): Array[ByteSeq] iso^ => _done('X') 

class PasswordMessage is ClientMessage
  let _base: ClientMessageBase delegate ClientMessage = ClientMessageBase
  new create(pass: String) => _write(pass)
  fun ref done(): Array[ByteSeq] iso^ => _done('p') 

class QueryMessage is ClientMessage
  let _base: ClientMessageBase delegate ClientMessage = ClientMessageBase
  new create(q: String) => _write(q)
  fun ref done(): Array[ByteSeq] iso^ =>_zero(); _done('Q') 

