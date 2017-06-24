use "pg/introspect"
use "pg/codec"

class Record
  let _desc: TupleDescription val
  let _tuple: Array[FieldData val] val

  new create(d: TupleDescription val, t: Array[FieldData val] val ) =>
    _desc = d
    _tuple = t

  fun apply(idx: ( USize | String )): PGValue ? =>
    (let pos: USize, let d: FieldDescription val) = _desc(idx)
    Decode(d.type_oid, _tuple(pos).data, d.format)
    // if false then error else I32(1) end

interface RecordCB
  fun apply(iter: Array[Record val] val)

type Rows is Array[Record val]
