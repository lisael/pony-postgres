"""
bench_pg.pony

Bench pg stuff.
"""

use "ponybench"
use "pg"

actor Main
  let bench: PonyBench
  new create(env: Env) =>
    bench = PonyBench(env)
    bench[I32]("Add", lambda(): I32 => I32(2) + 2 end, 1000)


