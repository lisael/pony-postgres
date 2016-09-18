use "debug"
use "crypto"

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

  be send_clear_pass(pass: String) =>
    _conn.writev(recover val PasswordMessage(pass).done() end)

  be send_md5_pass(pass: String, username: String, salt: Array[U8] val) =>
    // from PG doc : concat('md5', md5(concat(md5(concat(password, username)), random-salt)))
    var result = "md5" + ToHexString(
      MD5(
        ToHexString(MD5(pass+username)) + String.from_array(salt)
      )
    )
    // Debug(recover val ToHexString(MD5(pass+username)) + String.from_array(salt') end)
    // Debug(result)
    _conn.writev(recover val PasswordMessage(result).done() end)

  be got_md5_pass(pass: String, req: MD5PwdRequest val) =>
    Debug.out(pass)
    let that: AuthInteraction tag = this
    _pool.get_user(recover lambda(u: String)(that, pass, req) => that.send_md5_pass(pass, u, req.salt) end end)

  be message(m: ServerMessage val) =>
    let that: AuthInteraction tag = this
    match m
    | let r: ClearTextPwdRequest val =>
      _pool.get_pass(recover lambda(s: String)(that) => that.send_clear_pass(s) end end)
    | let r: MD5PwdRequest val  =>
      // _pool.log("md5Req")
      _pool.get_pass(recover lambda(s: String)(that, r) => that.got_md5_pass(s, r) end end)
    | let r: AuthenticationOkMessage val => None
    else
      log("Unknown ServerMessage")
    end

