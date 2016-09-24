use "debug"
type PGValue is (I64 | I32 | None)

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
  fun apply(type_oid: I32, value: Array[U8] val) ? => error
