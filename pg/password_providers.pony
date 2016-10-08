use "options"

interface PasswordProvider
  be apply(f: PassCB val)
  be chain(p: PasswordProvider tag)

actor RawPasswordProvider
  let _password: String

  new create(p: String) => _password = p
  be apply(f: PassCB val) => f(_password)
  be chain(p: PasswordProvider tag) => None

actor EnvPasswordProvider
  let _env: Env
  var _next: (PasswordProvider tag | None) = None

  new create(e: Env) => _env = e
  be chain(p: PasswordProvider tag) => _next = p
  be apply(f: PassCB val) =>
    try
      f(EnvVars(_env.vars())("PGPASSWORD"))
    else
      try (_next as PasswordProvider tag)(f) end
    end
