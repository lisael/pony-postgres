use "pg/introspect"

interface ServerMessage is Message

// pseudo messages
class ServerMessageBase is ServerMessage
class NullServerMessage is ServerMessage
class ConnectionClosedMessage is ServerMessage

// messages descirbed in https://www.postgresql.org/docs/current/static/protocol-message-formats.html
class AuthenticationOkMessage is ServerMessage
class ClearTextPwdRequest is ServerMessage
class EmptyQueryResponse is ServerMessage
class MD5PwdRequest is ServerMessage
  let salt: Array[U8] val
  new val create(salt': Array[U8] val)=>
    salt = salt'

class ErrorMessage is ServerMessage
  let items: Array[(U8, Array[U8] val)] val
  new val create(it: Array[(U8, Array[U8] val)] val) =>
    items = it

class ParameterStatusMessage is ServerMessage
  let key: String val
  let value: String val
  new val create(k: Array[U8] val, v: Array[U8] val) =>
    key = String.from_array(k)
    value = String.from_array(v)

class ReadyForQueryMessage is ServerMessage
  let status: TransactionStatus
  new val create(b: U8) =>
    status = StatusFromByte(b)

class BackendKeyDataMessage is ServerMessage
  let data: (U32, U32)
  new val create(pid: U32, key: U32) =>
    data = (pid,key)

class CommandCompleteMessage is ServerMessage
  let command: String
  new val create(c: String) => command = c

class RowDescriptionMessage is ServerMessage
  let row: RowDescription val
  new val create(rd: RowDescription val) => row = rd

class DataRowMessage is ServerMessage
  let fields: Array[FieldData val] val
  new val create(f: Array[FieldData val] val) => fields = f


