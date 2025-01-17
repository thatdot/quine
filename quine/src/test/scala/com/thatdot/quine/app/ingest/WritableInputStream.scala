package com.thatdot.quine.app

import java.io.{InputStream, PipedInputStream, PipedOutputStream}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, DurationInt}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.graph.{GraphService, QuineIdLongProvider}
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

  def makeGraph(graphName: String = "test-service"): GraphService = Await.result(
    GraphService(
      graphName,
      effectOrder = EventEffectOrder.PersistorFirst,
      persistorMaker = InMemoryPersistor.persistorMaker,
      idProvider = QuineIdLongProvider(),
    )(LogConfig.permissive),
    5.seconds,
  )

  def collect[T](src: Source[T, NotUsed])(implicit mat: Materializer): Seq[T] =
    Await.result(src.runWith(Sink.seq), Duration.Inf)

}
