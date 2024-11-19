package com.thatdot.quine.app.ingest

import scala.annotation.nowarn
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Success, Try}

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source, StreamConverters}
import org.apache.pekko.stream.testkit.TestSubscriber
import org.apache.pekko.stream.testkit.scaladsl.TestSink
import org.apache.pekko.util.{ByteString, Timeout}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import com.thatdot.quine.app.serialization.ProtobufSchemaCache
import com.thatdot.quine.app.{IngestTestGraph, QuineAppIngestControl, StdInStream, WritableInputStream}
import com.thatdot.quine.graph.cypher.Expr
import com.thatdot.quine.graph.{CypherOpsGraph, GraphService, LiteralOpsGraph, MasterStream, NamespaceId, idFrom}
import com.thatdot.quine.model.{PropertyValue, QuineId, QuineValue}
import com.thatdot.quine.routes.FileIngestFormat.CypherCsv
import com.thatdot.quine.routes.{FileIngestFormat, NumberIteratorIngest, StandardInputIngest}
import com.thatdot.quine.util.SwitchMode
import com.thatdot.quine.util.TestLogging._

class DelimitedIngestSrcDefTest extends AnyFunSuite with BeforeAndAfterAll {

  implicit val graph: GraphService = IngestTestGraph.makeGraph()
  implicit val system: ActorSystem = graph.system
  implicit val timeout: Timeout = Timeout(2.seconds)
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  val namespace: NamespaceId = None // Use default namespace
  implicit val noOpProtobufCache: ProtobufSchemaCache.Blocking.type = ProtobufSchemaCache.Blocking: @nowarn

  override def afterAll(): Unit = Await.result(graph.shutdown(), 1.second)

  abstract class LocalIngestTestContext[Q <: QuineValue](name: String, fileIngestFormat: FileIngestFormat)(implicit
    val graph: CypherOpsGraph,
  ) {

    def source: Source[ByteString, NotUsed] =
      StreamConverters
        .fromInputStream(() => writableInputStream.in)
        .mapMaterializedValue(_ => NotUsed)

    val ingestSrcDef: ContentDelimitedIngestSrcDef =
      ContentDelimitedIngestSrcDef.apply(
        SwitchMode.Open,
        fileIngestFormat,
        source,
        "UTF-8",
        10,
        1000,
        0,
        None,
        None,
        "local test",
        None,
      )

    val probe: TestSubscriber.Probe[MasterStream.IngestSrcExecToken] =
      ingestSrcDef.stream(namespace, _ => ()).toMat(TestSink.probe)(Keep.right).run()

    val fc: Future[QuineAppIngestControl] = ingestSrcDef.getControl

    protected def writeBytes(bytes: Array[Byte]): Unit = writableInputStream.writeBytes(bytes)

    private lazy val writableInputStream = new WritableInputStream

    /** Write a single test value we can distinguish by 'i', e.g. '{"A":i}'. */
    def writeValue(i: Int): Unit

    /** Define how to generate a quineId from the input 'i' value
      *
      * e.g   1 => idFrom("test", "json", 1)
      */
    def quineId(i: Int): QuineId

    /** Expected QuineValue resulting from input. */
    def matchingValue(i: Int): Q

    def retrieveResults(): Map[Int, Try[QuineValue]] = {

      (1 to 10).foreach(i => writeValue(i))
      probe.request(10)
      Thread.sleep(1000)

      val ctl: QuineAppIngestControl = Await.result(fc, Duration.Inf)
      val g = graph.asInstanceOf[LiteralOpsGraph]

      writableInputStream.close()

      Await.result(ctl.termSignal, 10.seconds)

      (1 to 10).map { i =>
        val prop: Map[Symbol, PropertyValue] = Await.result(g.literalOps(namespace).getProps(quineId(i)), 1.second)
        i -> prop.getOrElse(Symbol("value"), PropertyValue(QuineValue.Null)).deserialized
      }.toMap
    }
  }

  test("json to graph") {
    val ctx = new LocalIngestTestContext[QuineValue.Map](
      "json",
      FileIngestFormat.CypherJson(
        s"""MATCH (p) WHERE id(p) = idFrom('test','json', $$that.json) SET p.value = $$that RETURN (p)""",
      ),
    ) {
      override def writeValue(i: Int): Unit = writeBytes(s"${ujson.Obj("json" -> i.toString)}\n".getBytes())

      override def quineId(i: Int): QuineId =
        idFrom(Expr.Str("test"), Expr.Str("json"), Expr.Str(i.toString))(graph.idProvider)

      override def matchingValue(i: Int): QuineValue.Map = QuineValue.Map(Map("json" -> QuineValue.Str(i.toString)))
    }
    ctx.retrieveResults().foreach(e => assert(e._2 == Success(ctx.matchingValue(e._1))))
  }

  test("bytes to graph") {
    val ctx = new LocalIngestTestContext[QuineValue.Str](
      "bytes",
      FileIngestFormat.CypherLine(
        s"""MATCH (p) WHERE id(p) = idFrom('test','line', $$that) SET p.value = $$that RETURN (p)""",
      ),
    ) {
      override def writeValue(i: Int): Unit = writeBytes(s"===$i\n".getBytes())

      override def quineId(i: Int): QuineId =
        idFrom(Expr.Str("test"), Expr.Str("line"), Expr.Str(s"===$i"))(graph.idProvider)

      override def matchingValue(i: Int): QuineValue.Str = QuineValue.Str(s"===$i")
    }
    ctx.retrieveResults().foreach(e => assert(e._2 == Success(ctx.matchingValue(e._1))))
  }

  test("csv to graph") {
    //headers: Either[Boolean, List[String]] = Left(false),

    val ctx = new LocalIngestTestContext[QuineValue.Map](
      "csv",
      CypherCsv(
        s"""MATCH (p) WHERE id(p) = idFrom('test','csv', $$that.h2) SET p.value = $$that RETURN (p)""",
        "that",
        Right(List("h1", "h2")),
      ),
    ) {
      override def writeValue(i: Int): Unit = writeBytes(s"""A,$i\n""".getBytes)

      override def quineId(i: Int): QuineId =
        idFrom(Expr.Str("test"), Expr.Str("csv"), Expr.Str(i.toString))(graph.idProvider) //TODO

      override def matchingValue(i: Int): QuineValue.Map =
        QuineValue.Map(Map("h1" -> QuineValue.Str("A"), "h2" -> QuineValue.Str(i.toString)))

    }
    ctx.retrieveResults().foreach(e => assert(e._2 == Success(ctx.matchingValue(e._1))))
  }

  test("number format") {
    val d: IngestSrcDef = IngestSrcDef
      .createIngestSrcDef(
        "number input",
        None,
        NumberIteratorIngest(
          FileIngestFormat.CypherLine(
            s"""MATCH (x) WHERE id(x) = idFrom(toInteger($$that)) SET x.value = toInteger($$that)""",
          ),
          0,
          Some(11L),
          None,
          10,
        ),
        SwitchMode.Open,
      )
      .valueOr(_ => ???)
    val g = graph.asInstanceOf[LiteralOpsGraph]

    Await.ready(
      d.stream(namespace, _ => ())
        .runWith(Sink.ignore)
        .map { _ =>

          (1 to 10).foreach { i =>
            val prop: Map[Symbol, PropertyValue] =
              Await.result(g.literalOps(namespace).getProps(idFrom(Expr.Integer(i.toLong))(graph.idProvider)), 1.second)
            assert(
              prop.getOrElse(Symbol("value"), PropertyValue(QuineValue.Null)).deserialized == Success(
                QuineValue.Integer(i.toLong),
              ),
            )
          }

        },
      10.seconds,
    )
  }

  test("stdin") {

    val istream = new StdInStream()

    val d: IngestSrcDef = IngestSrcDef
      .createIngestSrcDef(
        "stdin",
        None,
        StandardInputIngest(
          FileIngestFormat.CypherLine(s"""MATCH (x) WHERE id(x) = idFrom("stdin", $$that) SET x.value = $$that"""),
          "UTF-8",
          10,
          1000,
          None,
        ),
        SwitchMode.Open,
      )
      .valueOr(_ => ???)
    val done = d.stream(namespace, _ => ()).toMat(Sink.ignore)(Keep.right).run()
    val fc = d.getControl
    val c: QuineAppIngestControl = Await.result(fc, 3.seconds)
    val g = graph.asInstanceOf[LiteralOpsGraph]
    (1 to 10).foreach(i => istream.writeBytes(s"$i\n".getBytes()))

    Thread.sleep(1000)
    Await.result(c.terminate(), 3.seconds)
    Await.ready(done, 3.seconds).map { _ =>

      (1 to 10).foreach { i =>

        val prop: Map[Symbol, PropertyValue] = Await
          .result(
            g.literalOps(namespace).getProps(idFrom(Expr.Str("stdin"), Expr.Str(i.toString))(graph.idProvider)),
            1.second,
          )
        assert(
          prop.getOrElse(Symbol("value"), PropertyValue(QuineValue.Null)).deserialized == Success(
            QuineValue.Str(i.toString),
          ),
        )
      }
    }
    istream.close()
  }
}
