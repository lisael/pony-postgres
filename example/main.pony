"""
pg.pony

Do pg stuff.
"""
use "pg"
use "net"
use "debug"

actor Main
  let sess: Session

  new create(env: Env) =>
    sess = Session(env where user="macflytest", password=EnvPasswordProvider(env), database="macflytest")
    sess.raw("SELECT 42, 24;;", recover val
      lambda(rows: Rows)(env, sess) =>
        for d in rows.desc.fields.values() do
          Debug.out(d.type_oid)
          Debug.out(d.name)
        end
        for row in rows.values() do
          for value in row.values() do
            try Debug.out(value as I32) else Debug.out("error...") end
          end
        end
        sess.terminate()
      end
    end)

