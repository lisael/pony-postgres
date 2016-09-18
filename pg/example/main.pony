"""
pg.pony

Do pg stuff.
"""
use "pg"
use "net"

actor Main
  new create(env: Env) =>
    let sess = Session(env where user="macflytest", password="macflytest", database="macflytest")
    sess.connect()
