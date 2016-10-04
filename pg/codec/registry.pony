use "buffered"
use "debug"

use "pg"

primitive TypeOid
  """ The type oids are found with:

    SELECT
        oid,
        typname
    FROM                  
        pg_catalog.pg_type
    WHERE                    
        typtype IN ('b', 'p')                                                 
        AND (typelem = 0 OR typname = '_oid' OR typname='_text' OR typlen > 0)
        AND oid <= 9999
    ORDER BY
        oid;

  """
  // TODO: Find NULL oid, i'm pretty sure it's not 0
  fun apply(t: None): I32 => 0
  fun apply(t: Bool val): I32 => 16
  fun apply(t: U8 val): I32 => 18
  fun apply(t: I64 val): I32 => 20
  fun apply(t: I16 val): I32 => 21
  fun apply(t: I32 val): I32 => 23
  fun apply(t: String val): I32 => 25
  fun apply(t: F32 val): I32 => 700
  fun apply(t: F64 val): I32 => 701
  /*fun apply(t: Any val): I32 => 0*/

primitive TypeOids
  fun apply(t: Array[PGValue] val): Array[I32] val =>
    recover val
      let result = Array[I32](t.size())
      for item in t.values() do
        try result.push(TypeOid(item) as I32) end
      end
      result
    end

primitive Decode
  fun apply(type_oid: I32, value: Array[U8] val, 0): PGValue ? =>
    DecodeText(type_oid, value)
  fun apply(type_oid: I32, value: Array[U8] val, 1): PGValue ? =>
    DecodeBinary(type_oid, value)
  fun apply(type_oid: I32, value: Array[U8] val, format: I16): PGValue ? => error

primitive DecodeText
  fun apply(23, value: Array[U8] val): I32 ? =>
    String.from_array(value).i32()
  fun apply(type_oid: I32, value: Array[U8] val) ? => Debug.out("Unknown type OID: " + type_oid.string()); error

primitive DecodeBinary
  fun apply(23, value: Array[U8] val): I32 ? => 
    var result = I32(0)
    for i in value.values() do
      result = (result << 8) + i.i32()
    end
    result
    
  fun apply(type_oid: I32, value: Array[U8] val) ? => Debug.out("Unknown type OID: " + type_oid.string()); error

primitive EncodeBinary
  fun apply(param: I32, writer: Writer) ? =>
    writer.i32_be(param)
  fun apply(param: PGValue, writer: Writer) ? => error
