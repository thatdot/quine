package com.thatdot.quine.ingest2.core

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.jdk.CollectionConverters._

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.model.ingest2.core.DataFoldableFrom

class AvroDecoderTest extends AnyFunSpec with Matchers {

  def canonicalize(v: Any): Any = v match {
    case b: Array[_] => b.toVector
    case m: Map[_, _] => m.view.mapValues(canonicalize).toMap
    case m: java.util.Map[_, _] => m.asScala.view.mapValues(canonicalize).toMap
    case bytes: ByteBuffer => bytes.array().toVector
    case _ => v
  }

  it("Avro - simple types") {
    val schema1 = new Schema.Parser().parse("""
        |{
        | "type": "record",
        | "name": "testRecord",
        | "fields": [
        |     {"name": "astring", "type": "string"},
        |     {"name": "anull", "type": "null"},
        |     {"name": "abool", "type": "boolean"},
        |     {"name": "aint", "type": "int"},
        |     {"name": "along", "type": "long"},
        |     {"name": "afloat", "type": "float"},
        |     {"name": "adouble", "type": "double"},
        |     {"name": "abytes", "type": "bytes"}
        |     ]
        |}
        |""".stripMargin)
    val record1: GenericData.Record = new GenericData.Record(schema1)
    val fieldVals = SortedMap[String, Any](
      ("astring" -> "string1"),
      ("anull" -> null),
      ("abool" -> true),
      ("aint" -> 100),
      ("along" -> Long.MaxValue),
      ("afloat" -> 101F),
      ("adouble" -> Double.MaxValue),
      ("abytes" -> ByteBuffer.wrap("some bytes".getBytes(StandardCharsets.UTF_8))),
    )
    fieldVals.foreach { case (s, v) => record1.put(s, v) }
    val result =
      DataFoldableFrom.avroDataFoldable.fold(record1, FoldableTestData.toAnyDataFolder).asInstanceOf[TreeMap[Any, Any]]
    assert(canonicalize(result) == canonicalize(fieldVals))
  }

  it("Avro - record of records") {
    val schema1 = new Schema.Parser().parse("""
        |{
        | "name": "multi",
        | "type": "record",
        | "fields": [
        |     {
        |       "name": "left",
        |       "type": {
        |         "name": "leftT",
        |         "type": "record",
        |         "fields": [ {"name": "leftA", "type": "string"}, {"name": "leftB", "type": "int"} ]
        |       }
        |     },
        |     {
        |       "name": "right",
        |       "type": {
        |         "name": "rightT",
        |         "type": "record",
        |         "fields": [ {"name": "rightA", "type": "boolean"}, {"name": "rightB", "type": "string"} ]
        |       }
        |     }
        |   ]
        |}
        |""".stripMargin)
    val left: GenericData.Record = new GenericData.Record(schema1.getField("left").schema())
    left.put("leftA", "a string")
    left.put("leftB", 101)
    val right: GenericData.Record = new GenericData.Record(schema1.getField("right").schema)
    right.put("rightA", false)
    right.put("rightB", "another string")
    val record: GenericData.Record = new GenericData.Record(schema1)
    record.put("left", left)
    record.put("right", right)
    val result = DataFoldableFrom.avroDataFoldable.fold(record, FoldableTestData.toAnyDataFolder)
    assert(
      result == TreeMap[String, TreeMap[String, Any]](
        ("left" -> TreeMap[String, Any](("leftA" -> "a string"), ("leftB" -> 101))),
        ("right" -> TreeMap[String, Any](("rightA" -> false), ("rightB" -> "another string"))),
      ),
    )
  }
  it("Avro - array of maps") {
    val schema1 = new Schema.Parser().parse("""
        | {
        |   "name": "ArrayOfMaps",
        |   "type": "record",
        |   "fields": [{
        |     "name": "alist",
        |     "type": {
        |       "type": "array",
        |       "items": {
        |         "type": "map",
        |         "values": "long"
        |       }
        |     }
        |   }]
        | }
        |""".stripMargin)
    val record: GenericData.Record = new GenericData.Record(schema1)
    val maps: List[java.util.Map[String, Long]] = List(
      Map(("k1a" -> 101L), ("k1b" -> 102L)).asJava,
      Map(("k2a" -> 102L), ("k2b" -> 103L)).asJava,
    )
    record.put(
      "alist",
      new GenericData.Array[java.util.Map[String, Long]](schema1.getField("alist").schema(), maps.asJava),
    )
    val result = DataFoldableFrom.avroDataFoldable.fold(record, FoldableTestData.toAnyDataFolder)
    assert(
      canonicalize(result) == Map(
        ("alist" -> List(
          Map(("k1a" -> 101), ("k1b" -> 102)),
          Map(("k2a" -> 102), ("k2b" -> 103)),
        )),
      ),
    )
  }
}
