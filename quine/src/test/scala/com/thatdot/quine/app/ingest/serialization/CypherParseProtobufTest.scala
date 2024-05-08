package com.thatdot.quine.app.ingest.serialization

import cats.implicits.toFunctorOps

import com.thatdot.quine.compiler.cypher.{CypherHarness, registerUserDefinedProcedure}
import com.thatdot.quine.graph.cypher.Expr

import ProtobufTest._

class CypherParseProtobufTest extends CypherHarness("procedure-parse-protobuf") {
  registerUserDefinedProcedure(new CypherParseProtobuf(testParserCache))

  val testPersonBytes: Expr.Bytes = Expr.Bytes(bytesFromURL(testPersonFile))
  val testPersonCypher: Expr.Map = Expr.Map(
    testReadablePerson
      .fmap(Expr.fromQuineValue)
  )

  describe("saving protobuf bytes as a property") {
    val query =
      """
         |MATCH (p) WHERE id(p) = idFrom("procedure-parse-protobuf", "bob")
         |SET p:Person,
         |    p.protobuf = $personBytes
         |WITH id(p) AS pId
         |MATCH (p) WHERE id(p) = pId
         |RETURN p.protobuf AS pbBytes
         |""".stripMargin

    testQuery(
      queryText = query,
      parameters = Map("personBytes" -> testPersonBytes),
      expectedColumns = Vector("pbBytes"),
      expectedRows = Seq(Vector(testPersonBytes)),
      expectedIsReadOnly = false,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = false
    )
  }

  describe("parseProtobuf procedure") {
    // from an in-memory value
    testQuery(
      """CALL parseProtobuf($personBytes, $schemaUrl, "Person") YIELD value RETURN value AS personDeserialized""",
      parameters = Map("personBytes" -> testPersonBytes, "schemaUrl" -> Expr.Str(addressBookSchemaFile.toString)),
      expectedColumns = Vector("personDeserialized"),
      expectedRows = Seq(Vector(testPersonCypher)),
      expectedIsReadOnly = true,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = false
    )
    // from a property
    testQuery(
      """
        |MATCH (p) WHERE id(p) = idFrom("procedure-parse-protobuf", "bob")
        |CALL parseProtobuf(p.protobuf, $schemaUrl, "Person") YIELD value RETURN value AS personDeserialized""".stripMargin,
      parameters = Map("personBytes" -> testPersonBytes, "schemaUrl" -> Expr.Str(addressBookSchemaFile.toString)),
      expectedColumns = Vector("personDeserialized"),
      expectedRows = Seq(Vector(testPersonCypher)),
      expectedIsReadOnly = true,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = false
    )
  }
}
