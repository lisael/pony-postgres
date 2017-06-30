use "pg/introspect"
use "pg/codec"
use "dbapi"

class Projection is DBRecord
  let _desc: TupleDescription val
  let _tuple: Array[FieldData val] val

  new create(d: TupleDescription val, t: Array[FieldData val] val ) =>
    _desc = d
    _tuple = t

  fun apply(idx: ( USize | String )): PGValue ? =>
    (let pos: USize, let d: FieldDescription val) = _desc(idx)
    Decode(d.type_oid, _tuple(pos).data, d.format)

interface ProjectionsHandler is DBRecordsHandler
