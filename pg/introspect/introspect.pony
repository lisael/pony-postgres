use "collections"

class FieldDescription
  let name: String
  let table_oid: I32
  let col_number: I16
  let type_oid: I32
  let type_size: I16
  let type_modifier: I32
  let format: I16

  new val create(name': String val,
                 table_oid': I32,
                 col_number': I16,
                 type_oid': I32,
                 type_size': I16,
                 type_modifier': I32,
                 format': I16) =>
    name = name'
    table_oid = table_oid'
    col_number = col_number'
    type_oid = type_oid'
    type_size = type_size'
    type_modifier = type_modifier'
    format = format'


class RowDescription
  let fields: Array[FieldDescription val]= Array[FieldDescription val]

  fun ref append(f: FieldDescription val) => fields.push(f)

class TupleDescription
  let _fields: Array[FieldDescription val] val
  let _by_name: Map[String, (USize, FieldDescription val)] = Map[String, (USize, FieldDescription val)]

  new create(fields: Array[FieldDescription val] val) =>
    _fields = fields
    var pos = USize(0)
    for d in fields.values() do
      _by_name.update(d.name, (pos, d))
      pos = pos + 1
    end

  fun apply(idx: (USize| String)): (USize, FieldDescription val) ? =>
    match idx
    | let idx': USize => (idx', _fields(idx'))
    | let idx': String => _by_name(idx')
    else
      error
    end

class FieldData
  let len: I32
  let data: Array[U8] val
  new create(l: I32, d: Array[U8] val) =>
    len = l
    data = d
