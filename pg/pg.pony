"""
pg.pony

Do pg stuff.
"""

actor Dopg
  new create(env: Env) =>
    env.out.print("Hello")

