use "pg/introspect"

interface ServerMessage is Message

// pseudo messages
class ServerMessageBase is ServerMessage
class NullServerMessage is ServerMessage
  fun string(): String => "Null"
class ConnectionClosedMessage is ServerMessage
  fun string(): String => "Connection closed"

// messages descirbed in https://www.postgresql.org/docs/current/static/protocol-message-formats.html
class AuthenticationOkMessage is ServerMessage
  fun string(): String => "Authentication OK"
class ClearTextPwdRequest is ServerMessage
  fun string(): String => "ClearTextPwdRequest"
class EmptyQueryResponse is ServerMessage
  fun string(): String => "Empty query"
class ParseCompleteMessage is ServerMessage
  fun string(): String => "Parse complete"
class BindCompleteMessage is ServerMessage
  fun string(): String => "Bind complete"
class CloseCompleteMessage is ServerMessage
  fun string(): String => "Close complete"
class MD5PwdRequest is ServerMessage
  let salt: Array[U8] val
  new val create(salt': Array[U8] val)=>
    salt = salt'
  fun string(): String => "MD5PwdRequest"

class ErrorMessage is ServerMessage
  let items: Array[(U8, Array[U8] val)] val
  new val create(it: Array[(U8, Array[U8] val)] val) =>
    items = it
  fun string(): String => "Error"

class ParameterStatusMessage is ServerMessage
  let key: String val
  let value: String val
  new val create(k: Array[U8] val, v: Array[U8] val) =>
    key = String.from_array(k)
    value = String.from_array(v)
  fun string(): String => "Param: " + key + "=" + value

class ReadyForQueryMessage is ServerMessage
  let status: TransactionStatus
  new val create(b: U8) =>
    status = StatusFromByte(b)
  fun string(): String => "Ready for query: " + status.string()

class BackendKeyDataMessage is ServerMessage
  let data: (U32, U32)
  new val create(pid: U32, key: U32) =>
    data = (pid,key)
  fun string(): String => "BackendKeyDataMessage"

class CommandCompleteMessage is ServerMessage
  let command: String
  new val create(c: String) => command = c
  fun string(): String => "CommandCompleteMessage"

class RowDescriptionMessage is ServerMessage
  let row: RowDescription val
  new val create(rd: RowDescription val) => row = rd
  fun string(): String => "RowDescriptionMessage"

class DataRowMessage is ServerMessage
  let fields: Array[FieldData val] val
  new val create(f: Array[FieldData val] val) => fields = f
  fun string(): String => "DataRowMessage"
