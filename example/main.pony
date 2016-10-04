"""
main.pony


"""
use "pg"
use "pg/codec"
use "net"
use "debug"

actor Main
  let sess: Session

  new create(env: Env) =>

    sess = Session(env where user="macflytest",
                   password=EnvPasswordProvider(env),
                   database="macflytest")

    let that = recover tag this end
    sess.raw("SELECT 42, 24;;",
             recover val
              lambda(r: Rows val)(that) =>
                  that.raw_handler(r)
              end
             end)

    sess.execute("SELECT $1, $2",
                 recover val [as PGValue: I32(70000), I32(-100000)] end,
                 recover val
                  lambda(r: Rows val)(that) =>
                      that.execute_handler(r)
                  end
                end)

  be raw_handler(rows: Rows val) =>
    for d in rows.desc.fields.values() do
      Debug.out(d.name)
    end
    for row in rows.values() do
      for value in row.values() do
        try Debug.out(value as I32) else Debug.out("error...") end
      end
    end

  be execute_handler(rows: Rows val) =>
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
