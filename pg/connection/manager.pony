use "collections"

use "pg"

actor ConnectionManager
  let _connections: Array[_Connection] = Array[_Connection tag]
  let _params: Array[(String, String)] val
  let _sess: Session tag
  let _host: String
  let _service: String
  let _user: String
  let _passwd_provider: PasswordProvider tag
  var _password: (String | None) = None

  new create(session: Session tag,
             host: String,
             service: String,
             user: String,
             passwd_provider: PasswordProvider tag,
             params: Array[Param] val) =>
    _params = params
    _sess = session
    _host = host
    _service = service
    _passwd_provider = passwd_provider
    _user = user

  be log(msg: String) =>
    _sess.log(msg)

  be connect(auth: AmbientAuth, f: {(Connection tag)} iso) =>
    let priv_conn=_Connection(auth, _host, _service, _params, this)
    _connections.push(priv_conn)
    let conn = Connection(priv_conn)
    priv_conn.set_frontend(conn)
    f(conn)

  be get_pass(f: PassCB iso) =>
    _passwd_provider(consume f)

  be get_user(f: UserCB iso) =>
    f(_user)

  be terminate() =>
    for i in Range(0, _connections.size()) do
      try _connections.pop().terminate() end
    end

