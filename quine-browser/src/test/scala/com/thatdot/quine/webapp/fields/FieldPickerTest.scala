package com.thatdot.quine.webapp.fields

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.webapp.fields.FieldPicker.{Selection, moveWithinRole}

class FieldPickerTest extends AnyFunSuite with Matchers {

  private def obs(k: String): Selection = Selection(FieldPath(Vector(PathSeg.Key(k))), "observation")
  private def tag(k: String): Selection = Selection(FieldPath(Vector(PathSeg.Key(k))), "tag")

  test("moves a selection down past its neighbour in the same role") {
    moveWithinRole(Vector(obs("a"), obs("b"), obs("c")), "observation", 0, 1) shouldBe
    Vector(obs("b"), obs("a"), obs("c"))
  }

  test("moves a selection up past its neighbour in the same role") {
    moveWithinRole(Vector(obs("a"), obs("b"), obs("c")), "observation", 2, -1) shouldBe
    Vector(obs("a"), obs("c"), obs("b"))
  }

  test("other roles' selections keep their positions") {
    moveWithinRole(Vector(obs("a"), tag("id"), obs("b")), "observation", 0, 1) shouldBe
    Vector(obs("b"), tag("id"), obs("a"))
  }

  test("a move past either end changes nothing") {
    val sels = Vector(obs("a"), obs("b"))
    moveWithinRole(sels, "observation", 0, -1) shouldBe sels
    moveWithinRole(sels, "observation", 1, 1) shouldBe sels
    moveWithinRole(sels, "observation", 5, 1) shouldBe sels
  }
}
