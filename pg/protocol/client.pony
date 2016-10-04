use "buffered"
use "debug"

use "pg/codec"
use "pg"

type _Param is (String, String)

interface ClientMessage is Message
  fun ref _zero()
  fun ref _write(s: String) 
  fun ref _u8(u: U8)
  fun ref _i32(i: I32)
  fun ref _i16(i: I16)
  fun ref _parameter(p: PGValue)
  fun ref _done(id: U8): Array[ByteSeq] iso^
  fun ref done(): Array[ByteSeq] iso^ => _done(0)
  fun ref _debug(id: U8): Array[ByteSeq] iso^

class ClientMessageBase is ClientMessage
  var _w: Writer = Writer
  var _out: Writer = Writer

  fun ref _zero() => _w.u8(0)
  fun ref _u8(u: U8) => _w.u8(u)
  fun ref _write(s: String) => _w.write(s)
  fun ref _i32(i: I32) => _w.i32_be(i)
  fun ref _i16(i: I16) => _w.i16_be(i)
  fun ref _parameter(p: PGValue) => 
    var param = recover ref Writer end
    try EncodeBinary(p, param) end
    _i32(param.size().i32())
    _w.writev(param.done())

  fun ref _debug(id: U8): Array[ByteSeq] iso^ =>
    if id != 0 then _out.u8(id) end
    _out.i32_be(_w.size().i32() + 4)
    _out.writev(_w.done())
    let out = Reader
    for s in _out.done().values() do
      try out.append(s as Array[U8] val) end
      try out.append((s  as String).array()) end
    end
    let w = Writer
    try
    for c in out.block(out.size()).values() do
      Debug.out(c)
      w.u8(c)
    end
    end
    w.done()

  fun ref _done(id: U8): Array[ByteSeq] iso^ =>
    if id != 0 then _out.u8(id) end
    _out.i32_be(_w.size().i32() + 4)
    _out.writev(_w.done())
    _out.done()

class NullClientMessage is ClientMessage
  fun ref _zero() => None
  fun ref _write(s: String)  => None
  fun ref _u8(u: U8) => None
  fun ref _i32(i: I32) => None
  fun ref _i16(i: I16) => None
  fun ref _done(id: U8): Array[ByteSeq] iso^ => recover Array[ByteSeq] end
  fun ref _debug(id: U8): Array[ByteSeq] iso^ => recover Array[ByteSeq] end
  fun ref _parameter(p: PGValue) => None

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

class FlushMessage
  let _base: ClientMessageBase delegate ClientMessage = ClientMessageBase
  fun ref done(): Array[ByteSeq] iso^ => _done('H') 

class SyncMessage
  let _base: ClientMessageBase delegate ClientMessage = ClientMessageBase
  fun ref done(): Array[ByteSeq] iso^ => _done('S') 

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

class DescribeMessage is ClientMessage
  let _base: ClientMessageBase delegate ClientMessage = ClientMessageBase

  new create(typ: U8, name: String) =>
    _u8(typ)
    _write(name)
    _zero()

  fun ref done(): Array[ByteSeq] iso^ => _done('D') 

class CloseMessage is ClientMessage
  let _base: ClientMessageBase delegate ClientMessage = ClientMessageBase

  new create(typ: U8, name: String) =>
    _u8(typ)
    _write(name)
    _zero()

  fun ref done(): Array[ByteSeq] iso^ => _done('C') 

class ParseMessage is ClientMessage
  let _base: ClientMessageBase delegate ClientMessage = ClientMessageBase

  new create(query: String, name: String, param_types: Array[I32]) =>
    _write(name)
    _zero()
    _write(query)
    _zero()
    _i16(param_types.size().i16())
    for oid in param_types.values() do
      _i32(oid)
    end

  fun ref done(): Array[ByteSeq] iso^ => _done('P') 

class BindMessage is ClientMessage
  let _base: ClientMessageBase delegate ClientMessage = ClientMessageBase

  new create(query: String, name: String, params: Array[PGValue] val) =>
    _write(name)
    _zero()
    _write(query)
    _zero()
    _i16(1)
    _i16(1)
    _i16(params.size().i16())
    for p in params.values() do
      _parameter(p)
    end
    _i16(1)
    _i16(1)

  fun ref done(): Array[ByteSeq] iso^ => _done('B') 

class ExecuteMessage is ClientMessage
  let _base: ClientMessageBase delegate ClientMessage = ClientMessageBase
 
  new create(portal: String, rows: USize) => 
    _write(portal)
    _zero()
    _i32(rows.i32())

  fun ref done(): Array[ByteSeq] iso^ => _done('E') 
