package com.thatdot.connect.importers.serialization

import java.net.URL
import java.nio.charset.StandardCharsets.UTF_8

import scala.util.Using

import com.google.common.io.ByteStreams
import com.google.protobuf.DynamicMessage
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.connect.serialization.{ConversionFailure, QuineValueToProtobuf}
import com.thatdot.quine.graph.cypher.Expr
import com.thatdot.quine.model.QuineValue

class ProtobufTest extends AnyFlatSpec with Matchers with EitherValues {

  def bytesFromURL(url: URL): Array[Byte] = Using.resource(url.openStream)(ByteStreams.toByteArray)

  def getClasspathResource(path: String): URL =
    getClass.getResource("/" + path)

  val schemaFile: URL = getClasspathResource("addressbook.desc")

  val protobufParser = new ProtobufParser(schemaFile, "Person")

  /** The protobuf_test.dat file was made by using scalapb to serialize a case class.
    * import com.google.protobuf.ByteString
    * import tutorial.addressbook.Person
    * import tutorial.addressbook.Person.PhoneType
    * import tutorial.addressbook.Person.TestOneof.NumPets
    * val record = Person("Bob", 10, Some("bob@example.com"), Seq(PhoneNumber("503-555-1234", PhoneType.MOBILE), PhoneNumber("360-555-1234", PhoneType.HOME)), Some(ByteString.copyFrom("foo".getBytes)), NumPets(8), Map("ANumber" -> 1.5.toFloat)
    * val outstream = new BufferedOutputStream(new FileOutputStream("/tmp/protobuf_test.data"))
    * record.writeTo(outstream)
    * outstream.close()
    */
  "The protobuf deserializer" should "map protobuf bytes to Cypher" in {
    val result = protobufParser.parseBytes(bytesFromURL(getClasspathResource("protobuf_test.dat")))
    result.getField("")("name") shouldBe Expr.Str("Bob")
    result.getField("")("id") shouldBe Expr.Integer(10L)
    result.getField("")("email") shouldBe Expr.Str("bob@example.com")
    result.getField("")("phones") shouldBe Expr.List(
      Vector(
        Expr.Map(Map("number" -> Expr.Str("503-555-1234"), "type" -> Expr.Str("MOBILE"))),
        Expr.Map(Map("number" -> Expr.Str("360-555-1234"), "type" -> Expr.Str("HOME")))
      )
    )
    result.getField("")("blob") shouldBe Expr.Bytes("foo".getBytes(UTF_8))
    result.getField("")("numPets") shouldBe Expr.Integer(8L)
    result.asMap("").get("petname") shouldBe None
    result.getField("")("mapField") shouldBe Expr.Map(
      Map(
        "ANumber" -> Expr.Floating(1.5)
      )
    )
  }

  val exampleQuineValue: Map[String, QuineValue] = Map(
    "name" -> QuineValue.Str("Bob"),
    "id" -> QuineValue.Integer(10L),
    "email" -> QuineValue.Str("bob@example.com"),
    "phones" -> QuineValue.List(
      Vector(
        QuineValue.Map(Map("number" -> QuineValue.Str("503-555-1234"), "type" -> QuineValue.Str("HOME")))
      )
    ),
    "garbage" -> QuineValue.Null
  )
  val protobufSerializer = new QuineValueToProtobuf(schemaFile, "Person")
  val maybeMessage: Either[ConversionFailure, DynamicMessage] = protobufSerializer.toProtobuf(exampleQuineValue)

  "The protobuf serializer" should "map QuineValue to a Protobuf DynamicMessage" in {

    val message = maybeMessage.value
    val List(name, id, email) = List("name", "id", "email").map(message.getDescriptorForType.findFieldByName)
    message.getField(name) shouldBe "Bob"
    message.getField(id) shouldBe 10L
    message.getField(email) shouldBe "bob@example.com"

  }

}
