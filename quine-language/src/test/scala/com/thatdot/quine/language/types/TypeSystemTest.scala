package com.thatdot.quine.language.types

import cats.data.NonEmptyList

import com.thatdot.quine.language.types.Constraint._
import com.thatdot.quine.language.types.Type._

class TypeSystemTest extends munit.FunSuite {

  test("primitive types creation") {
    val intType = PrimitiveType.Integer
    val realType = PrimitiveType.Real
    val booleanType = PrimitiveType.Boolean
    val stringType = PrimitiveType.String
    val nodeType = PrimitiveType.NodeType

    assert(intType.isInstanceOf[PrimitiveType])
    assert(realType.isInstanceOf[PrimitiveType])
    assert(booleanType.isInstanceOf[PrimitiveType])
    assert(stringType.isInstanceOf[PrimitiveType])
    assert(nodeType.isInstanceOf[PrimitiveType])
  }

  test("type factory methods") {
    assertEquals(Type.any, Any)
    assertEquals(Type.error, Error)
    assertEquals(Type.nullTy, Null)
  }

  test("effectful type wrapping") {
    val intType = PrimitiveType.Integer
    val effectfulInt = Effectful(intType)

    assertEquals(effectfulInt.valueType, intType)
    assert(effectfulInt.isInstanceOf[Type])
  }

  test("type variable creation") {
    val id = Symbol("x")
    val typeVar = TypeVariable(id, Constraint.None)

    assertEquals(typeVar.id, id)
    assertEquals(typeVar.constraint, Constraint.None)
  }

  test("type variable with constraints") {
    val id = Symbol("num")
    val numericVar = TypeVariable(id, Numeric)
    val semigroupVar = TypeVariable(id, Semigroup)

    assertEquals(numericVar.constraint, Numeric)
    assertEquals(semigroupVar.constraint, Semigroup)
  }

  test("type constructor creation") {
    val id = Symbol("List")
    val stringType = PrimitiveType.String
    val listOfString = TypeConstructor(id, NonEmptyList.one(stringType))

    assertEquals(listOfString.id, id)
    assertEquals(listOfString.args.head, stringType)
    assertEquals(listOfString.args.length, 1)
  }

  test("type constructor with multiple arguments") {
    val mapId = Symbol("Map")
    val keyType = PrimitiveType.String
    val valueType = PrimitiveType.Integer
    val mapType = TypeConstructor(mapId, NonEmptyList.of(keyType, valueType))

    assertEquals(mapType.args.length, 2)
    assertEquals(mapType.args.head, keyType)
    assertEquals(mapType.args.tail.head, valueType)
  }

  test("constraint hierarchy") {
    // Test constraint enum values
    assertEquals(Constraint.None.toString, "None")
    assertEquals(Numeric.toString, "Numeric")
    assertEquals(Semigroup.toString, "Semigroup")

    // All should be instances of Constraint
    assert(Constraint.None.isInstanceOf[Constraint])
    assert(Numeric.isInstanceOf[Constraint])
    assert(Semigroup.isInstanceOf[Constraint])
  }

  test("type equality") {
    val int1: Type = PrimitiveType.Integer
    val int2: Type = PrimitiveType.Integer
    val real1: Type = PrimitiveType.Real

    assertEquals(int1, int2)
    assertNotEquals(int1, real1)
  }

  test("type variable equality") {
    val id1 = Symbol("x")
    val id2 = Symbol("x")
    val id3 = Symbol("y")

    val var1 = TypeVariable(id1, Constraint.None)
    val var2 = TypeVariable(id2, Constraint.None)
    val var3 = TypeVariable(id3, Constraint.None)
    val var4 = TypeVariable(id1, Numeric)

    assertEquals(var1, var2)
    assertNotEquals(var1, var3)
    assertNotEquals(var1, var4)
  }

  test("effectful type equality") {
    val effect1 = Effectful(PrimitiveType.Integer)
    val effect2 = Effectful(PrimitiveType.Integer)
    val effect3 = Effectful(PrimitiveType.String)

    assertEquals(effect1, effect2)
    assertNotEquals(effect1, effect3)
  }

  test("type constructor equality") {
    val listId = Symbol("List")
    val stringType = PrimitiveType.String
    val intType = PrimitiveType.Integer

    val list1 = TypeConstructor(listId, NonEmptyList.one(stringType))
    val list2 = TypeConstructor(listId, NonEmptyList.one(stringType))
    val list3 = TypeConstructor(listId, NonEmptyList.one(intType))

    assertEquals(list1, list2)
    assertNotEquals(list1, list3)
  }

  test("type semigroup operation") {
    // Test the FIXME semigroup implementation
    val int = PrimitiveType.Integer
    val string = PrimitiveType.String

    val combined = Type.tsg.combine(int, string)

    // According to the FIXME implementation: (t1: Type, t2: Type) => t2
    assertEquals(combined, string)

    val combined2 = Type.tsg.combine(string, int)
    assertEquals(combined2, int)
  }

  test("semigroup identity") {
    // Test with various types
    val types = List(
      PrimitiveType.Integer,
      PrimitiveType.String,
      Any,
      Error,
      Null,
    )

    types.foreach { tpe =>
      val combined = Type.tsg.combine(tpe, tpe)
      assertEquals(combined, tpe, s"Combining $tpe with itself should return $tpe")
    }
  }

  test("complex type composition") {
    val personId = Symbol("Person")
    val nameField = PrimitiveType.String
    val ageField = PrimitiveType.Integer

    val personType = TypeConstructor(
      personId,
      NonEmptyList.of(nameField, ageField),
    )

    val effectfulPerson = Effectful(personType)

    assertEquals(effectfulPerson.valueType, personType)
    assert(effectfulPerson.valueType.isInstanceOf[TypeConstructor])
  }

  test("nested type constructors") {
    val listId = Symbol("List")
    val optionId = Symbol("Option")
    val intType = PrimitiveType.Integer

    val optionInt = TypeConstructor(optionId, NonEmptyList.one(intType))
    val listOfOptionInt = TypeConstructor(listId, NonEmptyList.one(optionInt))

    assertEquals(listOfOptionInt.args.head, optionInt)

    val nestedArg = listOfOptionInt.args.head.asInstanceOf[TypeConstructor]
    assertEquals(nestedArg.args.head, intType)
  }

  test("type pattern matching") {
    val types: List[Type] = List(
      PrimitiveType.Integer,
      Any,
      Error,
      Null,
      Effectful(PrimitiveType.String),
      TypeVariable(Symbol("x"), Constraint.None),
      TypeConstructor(Symbol("List"), NonEmptyList.one(PrimitiveType.Integer)),
    )

    types.foreach { tpe =>
      val category = tpe match {
        case _: PrimitiveType => "primitive"
        case Any => "any"
        case Error => "error"
        case Null => "null"
        case _: Effectful => "effectful"
        case _: TypeVariable => "variable"
        case _: TypeConstructor => "constructor"
      }

      assert(category.nonEmpty, s"Should categorize type: $tpe")
    }
  }

  test("symbol in types") {
    val id1 = Symbol("x")
    val id2 = Symbol("Person")
    val id3 = Symbol("result")

    val typeVar1 = TypeVariable(id1, Constraint.None)
    val typeVar2 = TypeVariable(id2, Constraint.None)
    val typeVar3 = TypeVariable(id3, Constraint.None)

    assertEquals(typeVar1.id, Symbol("x"))
    assertEquals(typeVar2.id, Symbol("Person"))
    assertEquals(typeVar3.id, Symbol("result"))
  }

  test("constraint validation scenarios") {
    // Test different constraint combinations that might occur in real usage
    val numericVar = TypeVariable(Symbol("n"), Numeric)
    val semigroupVar = TypeVariable(Symbol("s"), Semigroup)
    val unconstrainedVar = TypeVariable(Symbol("u"), Constraint.None)

    // All are valid type variables
    assert(numericVar.isInstanceOf[TypeVariable])
    assert(semigroupVar.isInstanceOf[TypeVariable])
    assert(unconstrainedVar.isInstanceOf[TypeVariable])

    // Constraints are properly set
    assertEquals(numericVar.constraint, Numeric)
    assertEquals(semigroupVar.constraint, Semigroup)
    assertEquals(unconstrainedVar.constraint, Constraint.None)
  }
}
