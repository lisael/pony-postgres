"""
pg.pony

Do pg stuff.
"""
use "pg"

actor Main
  new create(env: Env) =>
    env.out.print("Hello")

