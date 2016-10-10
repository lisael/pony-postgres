use "debug"
use "pg/connection"
use "pg/introspect"

interface FetchNotify
  fun ref descirption(desc: RowDescription) => None
  fun ref record(r: Record val) => None
  fun ref stop() => None
  fun size(): USize => 30

primitive _ReleasAfter
  fun apply(c: Connection tag, h: RecordCB val, records: Array[Record val] val) =>
    h(records)
    c.release()


actor Connection
  let _conn: BEConnection tag

  new create(c: BEConnection tag) =>
    _conn = c

  be execute(query: String,
             handler: RecordCB val,
             params: (Array[PGValue] val | None) = None) =>
    _conn.execute(query, recover val _ReleasAfter~apply(this, handler) end, params)

  be release() =>
    _conn.terminate()

  be do_terminate() =>
    Debug.out("Bye")

  be fetch(query: String, notify: FetchNotify iso,
           params: (Array[PGValue] val| None) = None) =>
    _conn.fetch(query, consume notify, params)
