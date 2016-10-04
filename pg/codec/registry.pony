use "buffered"
use "debug"

use "pg"


primitive Decode
  fun apply(type_oid: I32, value: Array[U8] val, 0): PGValue ? =>
    DecodeText(type_oid, value)
  fun apply(type_oid: I32, value: Array[U8] val, 1): PGValue ? =>
    DecodeBinary(type_oid, value)
  fun apply(type_oid: I32, value: Array[U8] val, format: I16): PGValue ? => error

primitive DecodeText
  fun apply(23, value: Array[U8] val): I32 ? =>
    String.from_array(value).i32()
  fun apply(type_oid: I32, value: Array[U8] val) ? => error

primitive DecodeBinary
  fun apply(23, value: Array[U8] val): I32 ? => 
    var result = I32(0)
    for i in value.values() do
      result = (result << 8) + i.i32()
    end
    result
    
  fun apply(type_oid: I32, value: Array[U8] val) ? => error

primitive EncodeBinary
  fun apply(param: I32, writer: Writer) ? =>
    writer.i32_be(param)
  fun apply(param: PGValue, writer: Writer) ? => error
