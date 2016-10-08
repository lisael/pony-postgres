use "debug"
use "pg/connection"

actor Connection
  let _conn: BEConnection tag

  new create(c: BEConnection tag) =>
    Debug.out("## Create Connection ##")
    _conn = c

  be execute(query: String,
             handler: RecordCB val,
             params: (Array[PGValue] val | None) = None) =>
    _conn.execute(query, handler, params)

  be release() =>
    Debug.out("## Terminate ##")
    _conn.terminate()

  be do_terminate() =>
    Debug.out("Bye")

  be fetch(query: String, notify: CursorNotify iso) =>
    Debug.out("######### Cursor ############")
