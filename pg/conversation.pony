trait Interaction
  
  be apply(c: Connection) 
  be message(m: ServerMessage val)

actor NullInteraction is Interaction
  
  be apply(c: Connection) => None
  be message(m: ServerMessage val)=> None


actor AuthInteraction is Interaction
  let _pool: ConnectionPool
  let _params: Array[Param] val
  let _conn: Connection
  new create(p: ConnectionPool, c: Connection, params: Array[Param] val) =>
    _pool=p
    _conn=c
    _params=params

  be log(msg: String) =>
    _pool.log(msg)

  be apply(c: Connection) =>
		log("connected")
    let data = recover val
    let msg = StartupMessage(_params)
    msg.done() 
    end
    c.writev(data)

  be message(m: ServerMessage val) => None

