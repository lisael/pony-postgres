use "debug"
use "pg/connection"
use "pg/introspect"
use "dbapi"


type FetchNotifyNext is {((FetchNotify iso | None))}

interface FetchNotify
  fun ref descirption(desc: RowDescription) => None
  fun ref record(r: Projection val) => None
  fun ref batch(records: Array[Projection val] val, next: FetchNotifyNext val) =>
    next(None)
    for r in records.values() do
      record(r)
    end
  fun ref stop() => None
  fun ref server_error() => None
  fun size(): USize => 0

primitive _ReleasAfter
  fun apply(c: Connection tag, h: ProjectionsHandler val, records: Array[Record val] val) =>
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
