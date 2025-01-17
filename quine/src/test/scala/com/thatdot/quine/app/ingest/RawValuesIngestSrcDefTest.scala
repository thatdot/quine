package com.thatdot.quine.app.ingest

import java.io.ByteArrayOutputStream
import java.util.Base64
import java.util.zip.{DeflaterOutputStream, GZIPOutputStream}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Framing, Keep, Source, StreamConverters}
import org.apache.pekko.stream.testkit.TestSubscriber
import org.apache.pekko.stream.testkit.scaladsl.TestSink
import org.apache.pekko.util.ByteString

import io.circe.Json
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.ingest.serialization.{ContentDecoder, CypherJsonInputFormat}
import com.thatdot.quine.app.{IngestTestGraph, ShutdownSwitch, WritableInputStream}
import com.thatdot.quine.graph.cypher.Value
import com.thatdot.quine.graph.{CypherOpsGraph, GraphService}
import com.thatdot.quine.util.{SwitchMode, Valve, ValveSwitch}

class RawValuesIngestSrcDefTest extends AnyFunSuite with BeforeAndAfterAll {

  implicit val graph: GraphService = IngestTestGraph.makeGraph()
  implicit val system: ActorSystem = graph.system

  override def afterAll(): Unit = Await.result(graph.shutdown(), 10.seconds)

  /** An ingest class that accepts data from a piped input stream
    * so that bytes can be directly written in tests.
    */
  case class TestJsonIngest(label: String, maxPerSecond: Option[Int] = None, decoders: Seq[ContentDecoder] = Seq())(
    implicit val graph: CypherOpsGraph,
  ) extends RawValuesIngestSrcDef(
        new CypherJsonInputFormat(
          s"""MATCH (p) WHERE id(p) = idFrom('test','$label', $$that.$label) SET p.value = $$that RETURN (p)""",
          "that",
        )(LogConfig.permissive),
        SwitchMode.Open,
        10,
        maxPerSecond,
        decoders,
        label,
        intoNamespace = None,
      ) {

    implicit protected val logConfig: LogConfig = LogConfig.permissive

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
    buildFrom: TestJsonIngest => Source[T, Mat],
  )(implicit val system: ActorSystem, implicit val graph: CypherOpsGraph) {
    val src: Source[T, Mat] = buildFrom(ingest)
    implicit val materializer: Materializer = graph.materializer
    val (mat, probe: TestSubscriber.Probe[T]) = src.toMat(TestSink.probe)(Keep.both).run()

    //val probe: TestSubscriber.Probe[T] = src.toMat(TestSink.probe)(Keep.both).run()

    def values(ct: Int): Seq[Json] = (1 to ct).map(i => Json.obj(ingest.name -> Json.fromString(i.toString)))

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
      Base64.getEncoder.encode(out.toByteArray)
    }

    val ctx = IngestTestContext(
      TestJsonIngest("deserialize", None, Seq(ContentDecoder.Base64Decoder, ContentDecoder.GzipDecoder)),
      i => i.undelimitedSource().via(i.deserializeAndMeter),
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
      Base64.getEncoder.encode(out.toByteArray)
    }

    val ctx = IngestTestContext(
      TestJsonIngest("deserialize", None, Seq(ContentDecoder.Base64Decoder, ContentDecoder.ZlibDecoder)),
      i => i.undelimitedSource().via(i.deserializeAndMeter),
    )

    //  Values are properly deserialized from json objects.
    ctx.values(10).foreach { obj =>
      val expected = s"$obj".getBytes()
      val encoded = base64ZlibEncode(expected)
      val decoded = ContentDecoder.ZlibDecoder.decode(ContentDecoder.Base64Decoder.decode(encoded))
      assert(decoded sameElements expected)
      ctx.ingest.write(encoded)
      val (value, bytes) = ctx.probe.requestNext(5.seconds)
      assert(value === Success(Value.fromJson(obj)))
      assert(bytes === ByteString(encoded))
    }
    ctx.close()
  }

  test("map deserialize") {

    val ctx = IngestTestContext(TestJsonIngest("deserialize"), i => i.source().via(i.deserializeAndMeter))

    //  Values are properly deserialized from json objects.
    ctx.values(10).foreach { obj =>
      ctx.ingest.write((obj.noSpaces + '\n').getBytes)
      val (value, bytes) = ctx.probe.requestNext(5.seconds)
      assert(value === Success(Value.fromJson(obj)))
      assert(bytes === ByteString(obj.noSpaces))
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
        i => i.source().via(i.deserializeAndMeter).via(i.throttle()),
      )
    val st: TestSubscriber.Probe[(Try[Value], ByteString)] = ctx.probe.request(10)
    ctx.writeValues(10)
    val ct = st.receiveWithin(2.seconds).size
    //1 off errors can sometimes happen in CI, need to allow for some imprecision
    assert(ct >= 5 && ct <= 8)
    ctx.close()
  }

  test("test switches") {
    val ctx: IngestTestContext[(Try[Value], ByteString), (ShutdownSwitch, Future[ValveSwitch])] =
      IngestTestContext(
        TestJsonIngest("switches", Some(1)),
        i =>
          i.sourceWithShutdown()
            .viaMat(Valve(SwitchMode.Open))(Keep.both),
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

}
