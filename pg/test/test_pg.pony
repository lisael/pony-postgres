"""
test_pg.pony

Test pg stuff.
"""

use "ponytest"
use "pg"

actor Main is TestList
  new create(env: Env) =>
    PonyTest(env, this)

  new make() =>
    None

  fun tag tests(test: PonyTest) =>
    test(_TestAdd)

class iso _TestAdd is UnitTest

  fun name():String => "Contains"

  fun apply(h: TestHelper) =>
    h.assert_eq[I32](2+2, 4)


