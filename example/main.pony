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
      lambda(r: Rows)(env, sess) =>
        env.out.print("Yay")
        for row in r.values() do
          for field in row.values() do
            Debug.out(field.len)
            try
              Debug.out(field.data(0))
              Debug.out(field.data(1))
            end
          end
        end
        sess.terminate()
      end
    end)

