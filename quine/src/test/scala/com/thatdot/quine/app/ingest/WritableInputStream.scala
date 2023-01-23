package com.thatdot.quine.app

import java.io.{InputStream, PipedInputStream, PipedOutputStream}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}

import akka.actor.ActorSystem
import akka.util.Timeout

import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.graph.{CypherOpsGraph, GraphService, QuineIdLongProvider}
import com.thatdot.quine.persistor.{EventEffectOrder, InMemoryPersistor}

/** An input stream that can be written to for testing input-stream based  ingest types. */
class WritableInputStream() extends AutoCloseable {
  val out = new PipedOutputStream()
  val in = new PipedInputStream(out)

  def writeBytes(bytes: Array[Byte]): Unit = {
    out.write(bytes)
    out.flush()
  }

  override def close(): Unit = {
    out.close()
    in.close()
  }
}

/** Wrap stdin in a [[WritableInputStream]]. Reset stdin on close. */
class StdInStream extends WritableInputStream() {

  val original: InputStream = System.in
  System.setIn(in)

  override def close(): Unit = {
    super.close()
    //not sure if this is necessary
    System.setIn(original)
  }
}

object IngestTestGraph {

  def meter(): IngestMeter = IngestMeter(
    "test",
    Metrics.meter("test_ct"),
    Metrics.meter("test_bytes")
  )

  def makeGraph(): GraphService = Await.result(
    GraphService(
      "test-service",
      effectOrder = EventEffectOrder.MemoryFirst,
      persistor = _ => InMemoryPersistor.empty,
      idProvider = QuineIdLongProvider()
    ),
    5.seconds
  )

  implicit val timeout: Timeout = 10.seconds
  implicit val graph: CypherOpsGraph = makeGraph()
  implicit val system: ActorSystem = graph.system
  implicit val ec: ExecutionContext = graph.system.dispatcher

}
