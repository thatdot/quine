package com.thatdot.quine.app.ingest

import java.io.ByteArrayOutputStream
import java.util.Base64
import java.util.zip.{DeflaterOutputStream, GZIPOutputStream}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.contrib.{SwitchMode, Valve, ValveSwitch}
import akka.stream.scaladsl.{Flow, Framing, Keep, Source, StreamConverters}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.util.ByteString

import org.scalatest.funsuite.AnyFunSuite
import ujson.Obj

import com.thatdot.quine.app.ingest.serialization.{ContentDecoder, CypherJsonInputFormat}
import com.thatdot.quine.app.{IngestTestGraph, ShutdownSwitch, WritableInputStream}
import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.graph.cypher.Value
class RawValuesIngestSrcDefTest extends AnyFunSuite {

  import com.thatdot.quine.app.IngestTestGraph._

  /** An ingest class that accepts data from a piped input stream
    * so that bytes can be directly written in tests.
    */
  case class TestJsonIngest(label: String, maxPerSecond: Option[Int] = None, decoders: Seq[ContentDecoder] = Seq())
      extends RawValuesIngestSrcDef(
        new CypherJsonInputFormat(
          s"""MATCH (p) WHERE id(p) = idFrom('test','$label', $$that.$label) SET p.value = $$that RETURN (p)""",
          "that"
        ),
        SwitchMode.Open,
        10,
        maxPerSecond,
        decoders,
        label
      )(IngestTestGraph.graph) {

    type InputType = ByteString

    /** Define a way to extract raw bytes from a single input event */
    def rawBytes(value: ByteString): Array[Byte] = value.toArray[Byte]

    def newlineDelimited(maximumLineSize: Int): Flow[ByteString, ByteString, NotUsed] = Framing
      .delimiter(ByteString("\n"), maximumLineSize, allowTruncation = true)
      .map(line => if (!line.isEmpty && line.last == '\r') line.dropRight(1) else line)

    /** Define a newline-delimited data source */
    def undelimitedSource(): Source[ByteString, NotUsed] =
      StreamConverters
        .fromInputStream(() => dataSource.in)
        .mapMaterializedValue(_ => NotUsed)

    /** Define a newline-delimited data source */
    override def source(): Source[ByteString, NotUsed] = undelimitedSource().via(newlineDelimited(10000))

    val dataSource = new WritableInputStream

    def write(bytes: Array[Byte]): Unit = dataSource.writeBytes(bytes)

    def close(): Unit = dataSource.close()
  }

  case class IngestTestContext[T, Mat](
    ingest: TestJsonIngest,
    buildFrom: TestJsonIngest => Source[T, Mat]
  )(implicit val system: ActorSystem, implicit val graph: CypherOpsGraph) {
    val src: Source[T, Mat] = buildFrom(ingest)
    implicit val materializer: Materializer = graph.materializer
    val (mat, probe: TestSubscriber.Probe[T]) = src.toMat(TestSink.probe)(Keep.both).run()

    //val probe: TestSubscriber.Probe[T] = src.toMat(TestSink.probe)(Keep.both).run()

    def values(ct: Int): Seq[Obj] = (1 to ct).map(i => ujson.Obj(ingest.name -> i.toString))

    def writeValues(ct: Int): Unit = values(ct).foreach { obj =>
      ingest.write(s"$obj\n".getBytes())
    }

    def close(): Unit = ingest.dataSource.close()

  }

  test("decompress gzip base64") {

    /** Gzipped, base64 encoded */
    def base64GzipEncode(bytes: Array[Byte]): Array[Byte] = {
      val out = new ByteArrayOutputStream
      val gzip = new GZIPOutputStream(out)
      gzip.write(bytes)
      gzip.close()
      return Base64.getEncoder.encode(out.toByteArray)
    }

    val ctx = IngestTestContext(
      TestJsonIngest("deserialize", None, Seq(ContentDecoder.Base64Decoder, ContentDecoder.GzipDecoder)),
      i => i.undelimitedSource().via(i.deserializeAndMeter)
    )

    //  Values are properly deserialized from json objects.
    ctx.values(10).foreach { obj =>
      val expected = s"$obj".getBytes()
      val encoded = base64GzipEncode(expected)
      val decoded = ContentDecoder.GzipDecoder.decode(ContentDecoder.Base64Decoder.decode(encoded))
      assert(decoded sameElements expected)
      ctx.ingest.write(encoded)
      val next: (Try[Value], ByteString) = ctx.probe.requestNext(5.seconds)
      assert(next._1 == Success(Value.fromJson(obj)))
      assert(next._2 == ByteString(encoded))
    }

    ctx.close()
  }

  test("decompress zlib base64") {

    /** Gzipped, base64 encoded */
    def base64ZlibEncode(bytes: Array[Byte]): Array[Byte] = {
      val out = new ByteArrayOutputStream
      val zlib = new DeflaterOutputStream(out)
      zlib.write(bytes)
      zlib.close()
      return Base64.getEncoder.encode(out.toByteArray)
    }

    val ctx = IngestTestContext(
      TestJsonIngest("deserialize", None, Seq(ContentDecoder.Base64Decoder, ContentDecoder.ZlibDecoder)),
      i => i.undelimitedSource().via(i.deserializeAndMeter)
    )

    //  Values are properly deserialized from json objects.
    ctx.values(10).foreach { obj =>
      val expected = s"$obj".getBytes()
      val encoded = base64ZlibEncode(expected)
      val decoded = ContentDecoder.ZlibDecoder.decode(ContentDecoder.Base64Decoder.decode(encoded))
      assert(decoded sameElements expected)
      ctx.ingest.write(encoded)
      val next: (Try[Value], ByteString) = ctx.probe.requestNext(5.seconds)
      assert(next._1 == Success(Value.fromJson(obj)))
      assert(next._2 == ByteString(encoded))
    }
    ctx.close()
  }

  test("map deserialize") {

    val ctx = IngestTestContext(TestJsonIngest("deserialize"), i => i.source().via(i.deserializeAndMeter))

    //  Values are properly deserialized from json objects.
    ctx.values(10).foreach { obj =>
      ctx.ingest.write(s"$obj\n".getBytes())
      val next: (Try[Value], ByteString) = ctx.probe.requestNext(5.seconds)
      assert(next._1 == Success(Value.fromJson(obj)))
      assert(next._2 == ByteString(obj.toString()))
    }

    /* Values that are not valid json properly return Failures */
    ctx.ingest.write("this is not valid json\n".getBytes)
    val next: (Try[Value], ByteString) = ctx.probe.requestNext(5.seconds)
    next._1 match {
      case Failure(_: org.typelevel.jawn.ParseException) => ()
      case e: Any => assert(false, s"bad value is not parse-able as json $e")
    }

    ctx.close()
  }

  test("throttle rate") {
    val ctx =
      IngestTestContext(
        TestJsonIngest("throttle", Some(3)),
        i => i.source().via(i.deserializeAndMeter).via(i.throttle())
      )
    val st: TestSubscriber.Probe[(Try[Value], ByteString)] = ctx.probe.request(10)
    ctx.writeValues(10)
    assert(st.receiveWithin(2.seconds).size == 6)
    ctx.close()
  }

  test("test switches") {
    val ctx: IngestTestContext[(Try[Value], ByteString), (ShutdownSwitch, Future[ValveSwitch])] =
      IngestTestContext(
        TestJsonIngest("switches", Some(1)),
        i =>
          i.sourceWithShutdown()
            .viaMat(Valve(SwitchMode.Open))(Keep.both)
      )

    val (_: ShutdownSwitch, valveFut: Future[ValveSwitch]) = ctx.mat
    val switch: ValveSwitch = Await.result(valveFut, 1.second)
    switch.flip(SwitchMode.Close)
    ctx.probe.request(10)
    ctx.writeValues(10)
    val messages = ctx.probe.receiveWithin(1.second, 10)
    assert(messages.isEmpty, "valve is closed, should not receive any values")
    Await.ready(switch.flip(SwitchMode.Open), 1.second)
    Thread.sleep(100)
    val messages2 = ctx.probe.receiveWithin(1.second, 10)
    assert(messages2.size == 10, "valve is open, should receive all 10 values")

  }
  test("test ack") {
    val ctx: IngestTestContext[(Try[Value], ByteString), NotUsed] =
      IngestTestContext(TestJsonIngest("ack", Some(4)), i => i.source().via(i.throttle()).via(i.deserializeAndMeter))
    val st: TestSubscriber.Probe[(Try[Value], ByteString)] = ctx.probe.request(10)
    ctx.writeValues(10)
    assert(st.receiveWithin(1.seconds).size == 4)
    ctx.close()
  }

}
