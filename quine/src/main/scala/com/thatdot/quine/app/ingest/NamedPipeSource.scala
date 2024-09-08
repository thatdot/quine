package com.thatdot.quine.app.ingest

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success, Try}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{FileIO, Source}
import org.apache.pekko.stream.stage._
import org.apache.pekko.stream.{Attributes, Outlet, SourceShape}
import org.apache.pekko.util.ByteString

import jnr.posix.POSIXFactory

import com.thatdot.quine.routes.FileIngestMode
import com.thatdot.quine.routes.FileIngestMode.NamedPipe
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.Log.implicits._
import com.thatdot.quine.util.QuineDispatchers

object NamedPipeSource extends LazySafeLogging {
  def fromPath(
    path: Path,
    chunkSize: Int = 8192,
    pollInterval: FiniteDuration = 1.second,
  ): Source[ByteString, NotUsed] =
    Source
      .fromGraph(new NamedPipeSource(path, chunkSize, pollInterval))
      .withAttributes(attributes)

  /** Factory for building a regular file source or a named path source from a file path.
    * @param path Path of file or named pipe
    * @param fileIngestMode If defined, explicitly determines if a regular file source or a named path sources should be used (otherwise the file status is auto detected)
    */
  def fileOrNamedPipeSource(
    path: Path,
    fileIngestMode: Option[FileIngestMode],
  )(implicit logConfig: LogConfig): Source[ByteString, NotUsed] = {
    val isNamedPipe = fileIngestMode map (_ == NamedPipe) getOrElse {
      try POSIXFactory.getPOSIX.stat(path.toString).isFifo
      catch {
        case e: IllegalStateException =>
          logger.warn(log"Unable to determine if path ${Safe(path)} is named pipe" withException e)
          false
      }
    }
    if (isNamedPipe) {
      logger.debug(safe"Using named pipe mode for reading ${Safe(path)}")
      NamedPipeSource.fromPath(path)
    } else
      FileIO.fromPath(path).mapMaterializedValue(_ => NotUsed)
  }

  private[this] val attributes = Attributes.name("namedPipeSource")
}

/** Uses a FileChannel to pull data from a named pipe. Reading from a named pipe is different
  * from reading from a regular file:
  *
  * - [[FileChannel]]#open and #read may block until data is available
  *
  * - Even after reading all the bytes in the file, the reader must tail for more data, because
  *   data may be appended to the named pipe at any time
  *
  * - Named pipes do not support seek, which is used by [[org.apache.pekko.stream.impl.io.FileSource]]
  *
  * @param path named pipe file name
  * @param chunkSize size of memory buffer allocated for this graph stage
  * @param pollInterval how long to wait before reopening and reading again after reading an EOF
  */
class NamedPipeSource(path: Path, chunkSize: Int, pollInterval: FiniteDuration)
    extends GraphStage[SourceShape[ByteString]] {
  require(chunkSize > 0, "chunkSize must be greater than 0")
  val out: Outlet[ByteString] = Outlet[ByteString]("NamedPipeSource.out")

  override val shape: SourceShape[ByteString] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with OutHandler {
      val buffer = ByteBuffer.allocate(chunkSize)

      /** File channel from through which data is read. Don't call `open` here
        * because it may be blocking!
        */
      var channel: Option[FileChannel] = None

      /** Handles the outcome of the async `FileChannel#open` triggered in
        * `onPull` (for when there is no open `FileChannel`)
        */
      private val openCallback = getAsyncCallback[Try[FileChannel]] {
        case Success(c) =>
          channel = Some(c)
          onPull()

        case Failure(ex) =>
          failStage(ex)
      }

      /** Handles the outcome of the async `FileChannel#read` triggered in
        * `onPull`
        */
      private val readCallback = getAsyncCallback[Try[Int]] {
        case Success(n) if n > 0 =>
          buffer.flip()
          val byteString = ByteString.fromByteBuffer(buffer)
          buffer.clear()
          emit(out, byteString)

        case Success(_) =>
          // 0 means no bytes read, -1 means end-of-stream. In either case,
          // wait a bit and then try to read again
          scheduleOnce("poll", pollInterval)

        case Failure(ex) =>
          failStage(ex)
      }

      var dispatcher: ExecutionContext = _

      setHandler(out, this)

      override def preStart(): Unit = {
        if (!Files.exists(path)) throw new NoSuchFileException(path.toString)
        require(!Files.isDirectory(path), s"Path '$path' is a directory")
        require(Files.isReadable(path), s"Missing read permission for '$path'")
        dispatcher = new QuineDispatchers(materializer.system).blockingDispatcherEC
      }

      override def onPull(): Unit = channel match {
        case None =>
          // Open the file (should happen only on the first `onPull`)
          dispatcher.execute { () =>
            openCallback.invoke(Try(FileChannel.open(path, StandardOpenOption.READ)))
          }
        case Some(c) =>
          // Read from the file
          dispatcher.execute { () =>
            readCallback.invoke(Try(c.read(buffer)))
          }
      }

      override def postStop(): Unit =
        for {
          c <- channel
        } {
          if (c.isOpen()) {
            c.close()
          }
          channel = None
        }

      override def onTimer(timerKey: Any): Unit = timerKey match {
        case "poll" => onPull()
        case _ => throw new Exception(s"Unhandled timer key $timerKey")
      }
    }

  override def toString: String = s"NamedPipeSource($path, $chunkSize, $pollInterval)"
}
