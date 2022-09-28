package com.thatdot.quine.bolt

import scala.util.Try

import akka.NotUsed
import akka.stream.scaladsl._
import akka.util.{ByteString, ByteStringBuilder, Timeout}

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.BuildInfo
import com.thatdot.quine.compiler.cypher
import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.graph.cypher._
import com.thatdot.quine.model.QuineIdProvider

object Protocol extends LazyLogging {

  /** Possible states of a BOLT connection.
    */
  sealed abstract class State {
    final type Handler =
      PartialFunction[(ProtocolMessage, Source[Record, NotUsed]), State.HandlerResult]

    def handleMessage(implicit graph: CypherOpsGraph, timeout: Timeout): Handler
  }

  object State extends LazyLogging {
    object HandlerResult {
      def apply(nextState: State, response: ProtocolMessage): HandlerResult =
        HandlerResult(nextState, Source.single(response))
      def apply(
        nextState: State,
        response: ProtocolMessage,
        resultsQueue: Source[Record, NotUsed]
      ): HandlerResult =
        HandlerResult(nextState, Source.single(response), resultsQueue)
    }
    final case class HandlerResult(
      nextState: State,
      response: Source[ProtocolMessage, NotUsed] = Source.empty,
      resultsQueue: Source[Record, NotUsed] = Source.empty
    )
    case object Uninitialized extends State {
      override def handleMessage(implicit graph: CypherOpsGraph, timeout: Timeout): Handler = {

        // TODO: actual authentication
        case (Init(_, _), _) => // in the Uninitialized state, there cannot be a results buffer
          // This is so that `cypher-shell` can work
          val version = "Neo4j/" + BuildInfo.version.replaceAll("\\+[0-9]{8}-DIRTY$", "")
          HandlerResult(
            State.Ready,
            Success(
              Map(
                "db" -> Expr.Str("quine"),
                "server" -> Expr.Str(version)
              )
            )
          ) // TODO authorization, quine version string}
      }
    }

    case object Ready extends State {
      override def handleMessage(implicit graph: CypherOpsGraph, timeout: Timeout): Handler = {
        // in the Ready state, there cannot be a buffer, but Source.empty is unstable so we can't match on it
        case (Run(statement, parameters), _) =>
          Try {
            // TODO: remove `PROFILE` here too
            val ExplainedQuery = raw"(?is)\s*explain\s+(.*)".r
            val (explained, cleanedStatement) = statement match {
              case ExplainedQuery(query) => true -> query
              case other => false -> other
            }
            val queryResult: QueryResults = cypher.queryCypherValues(
              cleanedStatement,
              parameters
            )

            val fields = queryResult.columns.map(col => Expr.Str(col.name))
            val plan = cypher.Plan.fromQuery(queryResult.compiled.query)
            val resultAvailableAfter = 1L // milliseconds after which results may be requested

            if (explained) {
              logger.debug(s"User requested EXPLAIN of query: ${queryResult.compiled.query}")
              // EXPLAIN'ed results do not get executed
              (
                Success(
                  Map(
                    "db" -> Expr.Str("quine"),
                    "plan" -> plan.toValue,
                    "result_available_after" -> Expr.Integer(resultAvailableAfter)
                  )
                ),
                Source.empty[Record]
              )
            } else {
              (
                Success(
                  Map(
                    "db" -> Expr.Str("quine"),
                    "fields" -> Expr.List(fields),
                    "result_available_after" -> Expr.Integer(resultAvailableAfter)
                  )
                ),
                queryResult.results.map(Record.apply)
              )
            }
          } match {
            case scala.util.Success((successMsg, resultsQueue)) =>
              HandlerResult(State.Streaming, successMsg, resultsQueue)

            case scala.util.Failure(error) =>
              HandlerResult(
                State.Failed,
                Failure(
                  Map(
                    "message" -> Expr.Str(error.getMessage),
                    "code" -> Expr.Str(error.getClass.getName)
                  )
                )
              )
          }
      }
    }

    /** The server has results queued and ready for streaming
      */
    case object Streaming extends State {
      override def handleMessage(implicit graph: CypherOpsGraph, timeout: Timeout): Handler = {
        case (PullAll(), queryResults) =>
          val response: Source[ProtocolMessage, NotUsed] = queryResults
            .concat(
              Source.single(
                Success(
                  Map(
                    "db" -> Expr.Str("quine"),
                    "result_consumed_after" -> Expr
                      .Integer(1) // milliseconds from when results were made available to when results were pulled
                  )
                )
              )
            )
            .recover {
              case err: CypherException =>
                Failure(
                  Map(
                    "message" -> Expr.Str(err.getMessage),
                    "code" -> Expr.Str(err.getClass.getName)
                  )
                )
              case err =>
                logger.error(
                  s"Cypher handler threw unexpected error while streaming results to client: $err"
                )
                throw err // TODO possibly terminate connection
            }
          HandlerResult(State.Ready, response, Source.empty)
        case (DiscardAll(), _) =>
          HandlerResult(
            State.Ready,
            Success(
              Map(
                "db" -> Expr.Str("quine"),
                "result_consumed_after" -> Expr
                  .Integer(
                    1
                  ) // milliseconds from when results were made available to when results were pulled
              )
            ),
            Source.empty
          )
      }
    }

    case object Failed extends State {
      override def handleMessage(implicit graph: CypherOpsGraph, timeout: Timeout): Handler = {
        case (AckFailure(), resultsQueue) =>
          HandlerResult(State.Ready, Success(), resultsQueue)
        case (Run(_, _), resultsQueue) =>
          HandlerResult(State.Ready, Ignored(), resultsQueue)
        case (DiscardAll(), resultsQueue) =>
          HandlerResult(State.Ready, Ignored(), resultsQueue)
        case (PullAll(), resultsQueue) =>
          HandlerResult(State.Ready, Ignored(), resultsQueue)
      }
    }

    // This state should be unreachable given the nature of akka-streams
//    case object Interrupted extends State {
//      override def handleMessage(implicit graph: CypherOperations, timeout: Timeout) = ???
//    }

    case object Defunct extends State {
      override def handleMessage(implicit graph: CypherOpsGraph, timeout: Timeout): Handler =
        PartialFunction.empty
    } // This state means the connection is terminated
  }

  /** Server-side of the Bolt protocol
    *
    * Bytes coming from the client are expected to be routed into the flow and
    * the server's responses will be coming out of the flow.
    */
  def bolt(implicit
    graph: CypherOpsGraph,
    timeout: Timeout
  ): Flow[ByteString, ByteString, NotUsed] =
    Protocol.handleMessages
      .join(Protocol.protocolMessageSerialization(graph.idProvider))
      .join(Protocol.messageTransferEncoding)
      .join(Protocol.handshake)

  /** Handshake
    *
    *   - Looks for (and strips off) header bytes. Subsequent bytes are passed
    *     through directly
    *   - Outputs the chosen version (00 00 00 00 if none and close)
    */
  val handshake: BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] = {
    var handshakeSucceeded = false
    BidiFlow.fromFlows(
      Flow[ByteString].prepend {
        Source.single(ByteString(0x00, 0x00, 0x00, 0x01))
        //        if (handshakeSucceeded) {
        //          Source.single(ByteString(0x00, 0x00, 0x00, 0x01))
        //        }
        //        else {
        //          Source.single(ByteString(0x00, 0x00, 0x00, 0x00))
        //        }
      },
      Flow[ByteString].statefulMapConcat { () =>
        val preamble = ByteString(0x60, 0x60, 0xB0, 0x17)
        val handshakeLength =
          4 * 4 + preamble.length // 4 32-bit (4-byte) integers, plus the magic int
        var byteStringHeader = ByteString.empty
        var processedHeader = false

        {
          case bstr if processedHeader => List(bstr)
          case bstr =>
            byteStringHeader ++= bstr
            if (byteStringHeader.length >= handshakeLength) {
              processedHeader = true
              val (header, rest) = byteStringHeader.splitAt(handshakeLength)
              if (
                header.take(4) != preamble
                || !header.grouped(4).contains(ByteString(0x00, 0x00, 0x00, 0x01))
              ) {
                logger.info(
                  s"Handshake ${header.toPrettyString} received from client did not pass. The rest was ${rest.toPrettyString}. Full string on next line.\n${(header ++ rest).toHexString}"
                )
                ???
                // TODO somehow kill connection and respond with ByteString(0x00, 0x00, 0x00, 0x00)
              } else {
                handshakeSucceeded = true
                logger.debug("Received valid BOLT handshake supporting version 1")
              }
              List(rest)
            } else {
              List.empty
            }
        }
      }
    )
  }

  /** Message transfer encoding
    *
    * [2 bytes indicating message length][message bytes][0x00 x00 footer]
    */
  val messageTransferEncoding: BidiFlow[MessageBytes, ByteString, ByteString, MessageBytes, NotUsed] = {
    val LENGTH_BYTES = 2
    val TERMINATOR = ByteString(0x00, 0x00)
    BidiFlow.fromFlows(
      Flow[MessageBytes].map { case MessageBytes(messageData) =>
        // prepend the length of the message, append the terminator
        val header = new ByteStringBuilder()
          .putLongPart(messageData.length.toLong, LENGTH_BYTES)(java.nio.ByteOrder.BIG_ENDIAN)
          .result()

        header ++ messageData ++ TERMINATOR
      },
      Framing
        .lengthField(
          fieldLength = LENGTH_BYTES,
          fieldOffset = 0,
          maximumFrameLength = 2 << 15,
          byteOrder = java.nio.ByteOrder.BIG_ENDIAN,
          computeFrameSize = { (_, x) =>
            x + LENGTH_BYTES + TERMINATOR.length
          }
        )
        .map(
          _.drop(LENGTH_BYTES).dropRight(TERMINATOR.length)
        ) // drop the length bytes and the terminator
        .map(MessageBytes.apply) // wrap the bytes in a Protocol.Message
    )
  }

  /** Protocol messaging layer */
  def protocolMessageSerialization(implicit
    idProvider: QuineIdProvider
  ): BidiFlow[ProtocolMessage, MessageBytes, MessageBytes, ProtocolMessage, NotUsed] = {

    /** Handles the messaging layer */
    val boltSerialization = Serialization()
    BidiFlow.fromFlows(
      Flow.fromFunction[ProtocolMessage, MessageBytes] { (protocolMessage: ProtocolMessage) =>
        MessageBytes {
          boltSerialization
            .writeFull(ProtocolMessage.writeToBuffer(boltSerialization))(protocolMessage)
        }
      },
      Flow.fromFunction[MessageBytes, ProtocolMessage] { case MessageBytes(data) =>
        val structureTry = Try(boltSerialization.readFull(boltSerialization.readStructure)(data))
        val structure = structureTry match {
          case scala.util.Success(strct) => strct
          case scala.util.Failure(err) =>
            throw BoltSerializationException(
              s"Failed to deserialize message with bytes: $data",
              err
            )
        }

        ProtocolMessage.decodeStructure(structure) match {
          case Some(msg) => msg
          case None =>
            throw new BoltSerializationException(
              s"Failed to decode message with signature ${structure.signature}: $structure"
            )
        }
      }
    )
  }

  def handleMessages(implicit
    graph: CypherOpsGraph,
    timeout: Timeout
  ): Flow[ProtocolMessage, ProtocolMessage, NotUsed] =
    handleMessagesToSources(graph, timeout).flatMapConcat(identity)

  def handleMessagesToSources(implicit
    graph: CypherOpsGraph,
    timeout: Timeout
  ): Flow[ProtocolMessage, Source[ProtocolMessage, NotUsed], NotUsed] =
    Flow[ProtocolMessage].statefulMapConcat { () =>
      var connectionState: State = State.Uninitialized

      var queryResults: Source[Record, NotUsed] = Source.empty

      (msg: ProtocolMessage) =>
        logger.debug(s"Received message $msg")
        val State.HandlerResult(newState, response, resultsQueue) = connectionState
          .handleMessage(graph, timeout)
          .applyOrElse[(ProtocolMessage, Source[Record, NotUsed]), State.HandlerResult](
            (msg, queryResults),
            {
              case (Reset(), _) =>
                State.HandlerResult(State.Ready, Success(), Source.empty)
              case _ =>
                logger.warn(
                  s"Received message that is invalid for current BOLT protocol state: $connectionState (logged at INFO level)."
                )
                logger.info(
                  s"Received message that is invalid for current BOLT protocol state $connectionState. Message: $msg"
                )
                State.HandlerResult(State.Defunct)
            }
          )
        if (newState == State.Defunct) {
          logger.error(
            "Message handler explicitly indicated the connection should be killed"
          ) // TODO actually terminate
        }
        connectionState = newState
        queryResults = resultsQueue
        logger.debug(s"Returning messages $response")
        Vector(response)
    }

  /** A BOLT protocol-encoded message ByteString, free of length or terminator
    *
    * @param messageData
    */
  final case class MessageBytes(messageData: ByteString) {
    override def toString: String = s"MessageBytes(${messageData.toHexString})"
  }

  /** A parsed / serializable BOLT protocol message
    */
  sealed abstract class ProtocolMessage

  /** Helper object for [de]serializing ProtocolMessage instances.
    * TODO [[ProtocolMessage.decodeStructure]] and [[ProtocolMessage.writeToBuffer]] must be updated for each subtype
    */
  object ProtocolMessage {
    def decodeStructure(struct: Structure)(implicit idp: QuineIdProvider): Option[ProtocolMessage] =
      (struct.signature match {
        case Init.InitStructure.signature => Some(Init.InitStructure)
        case Success.SuccessStructure.signature => Some(Success.SuccessStructure)
        case Failure.FailureStructure.signature => Some(Failure.FailureStructure)
        case AckFailure.AckFailureStructure.signature => Some(AckFailure.AckFailureStructure)
        case Ignored.IgnoredStructure.signature => Some(Ignored.IgnoredStructure)
        case Reset.ResetStructure.signature => Some(Reset.ResetStructure)
        case Run.RunStructure.signature => Some(Run.RunStructure)
        case PullAll.PullAllStructure.signature => Some(PullAll.PullAllStructure)
        case DiscardAll.DiscardAllStructure.signature => Some(DiscardAll.DiscardAllStructure)
        case Record.RecordStructure.signature => Some(Record.RecordStructure)
        case _ => None
      }).map(_.fromStructure(struct))

    /** Write the ProtocolMessage as a ByteString to the provided ByteStringBuilder, using the provided serializer.
      * This is meant as a parameter to [[Serialization.writeFull]]
      *
      * @param s
      * @param buf
      * @param msg
      */
    def writeToBuffer(s: Serialization)(buf: ByteStringBuilder, msg: ProtocolMessage): Unit =
      msg match {
        case initMsg: Init => s.writeStructure(buf, initMsg)
        case successMsg: Success => s.writeStructure(buf, successMsg)
        case failureMsg: Failure => s.writeStructure(buf, failureMsg)
        case ackFailureMsg: AckFailure => s.writeStructure(buf, ackFailureMsg)
        case ignoredMsg: Ignored => s.writeStructure(buf, ignoredMsg)
        case resetMsg: Reset => s.writeStructure(buf, resetMsg)
        case runMsg: Run => s.writeStructure(buf, runMsg)
        case pullAllMsg: PullAll => s.writeStructure(buf, pullAllMsg)
        case discardAllMsg: DiscardAll => s.writeStructure(buf, discardAllMsg)
        case recordMsg: Record => s.writeStructure(buf, recordMsg)
      }
  }

  final case class Reset() extends ProtocolMessage

  object Reset {

    implicit object ResetStructure extends Structured[Reset] {

      def fromStructure(structure: Structure)(implicit idp: QuineIdProvider): Reset = {
        assert(structure.signature == signature, "Wrong signature for RESET")
        structure.fields match {
          case Nil => Reset()

          case _ =>
            throw new IllegalArgumentException(
              s"Structure with signature of RESET has the wrong schema"
            )
        }
      }

      val signature: Byte = 0x0F.toByte

      def fields(reset: Reset)(implicit idp: QuineIdProvider): List[Value] = Nil

    }

  }

  /** @see <https://boltprotocol.org/v1/#message-init> */
  final case class Init(
    clientName: String,
    authToken: Option[Map[String, Value]]
  ) extends ProtocolMessage

  object Init {

    implicit object InitStructure extends Structured[Init] {

      def fromStructure(structure: Structure)(implicit idp: QuineIdProvider): Init = {
        assert(structure.signature == signature, "Wrong signature for INIT")
        structure.fields match {
          case List(
                Expr.Str(clientName),
                Expr.Map(authToken)
              ) =>
            Init(clientName, Some(authToken))

          case List(
                Expr.Str(clientName),
                Expr.Null
              ) =>
            Init(clientName, None)

          case _ =>
            throw new IllegalArgumentException(
              s"Structure with signature of INIT has the wrong schema"
            )
        }
      }

      val signature: Byte = 0x01.toByte

      def fields(init: Init)(implicit idp: QuineIdProvider): List[Value] = List(
        Expr.Str(init.clientName),
        init.authToken match {
          case None => Expr.Null
          case Some(authToken) => Expr.Map(authToken)
        }
      )

    }

  }

  final case class Success(metadata: Map[String, Value] = Map()) extends ProtocolMessage

  object Success {

    implicit object SuccessStructure extends Structured[Success] {

      def fromStructure(structure: Structure)(implicit idp: QuineIdProvider): Success = {
        assert(structure.signature == signature, "Wrong signature for SUCCESS")
        structure.fields match {
          case Expr.Map(meta) :: Nil => Success(meta)

          case _ =>
            throw new IllegalArgumentException(
              s"Structure with signature of SUCCESS has the wrong schema"
            )
        }
      }

      val signature: Byte = 0x70.toByte

      def fields(success: Success)(implicit idp: QuineIdProvider): List[Value] = List(
        Expr.Map(success.metadata)
      )

    }

  }

  final case class Failure(metadata: Map[String, Value] = Map()) extends ProtocolMessage

  object Failure {

    implicit object FailureStructure extends Structured[Failure] {

      def fromStructure(structure: Structure)(implicit idp: QuineIdProvider): Failure = {
        assert(structure.signature == signature, "Wrong signature for FAILURE")
        structure.fields match {
          case Expr.Map(meta) :: Nil => Failure(meta)

          case _ =>
            throw new IllegalArgumentException(
              s"Structure with signature of FAILURE has the wrong schema"
            )
        }
      }

      val signature: Byte = 0x7F.toByte

      def fields(failure: Failure)(implicit idp: QuineIdProvider): List[Value] = List(
        Expr.Map(failure.metadata)
      )

    }

  }

  final case class AckFailure() extends ProtocolMessage

  object AckFailure {

    implicit object AckFailureStructure extends Structured[AckFailure] {

      def fromStructure(structure: Structure)(implicit idp: QuineIdProvider): AckFailure = {
        assert(structure.signature == signature, "Wrong signature for ACK_FAILURE")
        structure.fields match {
          case Nil => AckFailure()

          case _ =>
            throw new IllegalArgumentException(
              s"Structure with signature of ACK_FAILURE has the wrong schema"
            )
        }
      }

      val signature: Byte = 0x0E.toByte

      def fields(ackFailure: AckFailure)(implicit idp: QuineIdProvider): List[Value] = Nil

    }

  }

  final case class Ignored() extends ProtocolMessage

  object Ignored {

    implicit object IgnoredStructure extends Structured[Ignored] {

      def fromStructure(structure: Structure)(implicit idp: QuineIdProvider): Ignored = {
        assert(structure.signature == signature, "Wrong signature for IGNORED")
        structure.fields match {
          case Nil => Ignored()

          case _ =>
            throw new IllegalArgumentException(
              s"Structure with signature of IGNORED has the wrong schema"
            )
        }
      }

      val signature: Byte = 0x7E.toByte

      def fields(ignored: Ignored)(implicit idp: QuineIdProvider): List[Value] = Nil

    }

  }

  final case class Run(statement: String, parameters: Map[String, Value]) extends ProtocolMessage

  object Run {

    implicit object RunStructure extends Structured[Run] {

      def fromStructure(structure: Structure)(implicit idp: QuineIdProvider): Run = {
        assert(structure.signature == signature, "Wrong signature for RUN")
        structure.fields match {
          case Expr.Str(statement) :: Expr.Map(parameters) :: Nil => Run(statement, parameters)

          case _ =>
            throw new IllegalArgumentException(
              s"Structure with signature of RUN has the wrong schema"
            )
        }
      }

      val signature: Byte = 0x10.toByte

      def fields(run: Run)(implicit idp: QuineIdProvider): List[Value] = List(
        Expr.Str(run.statement),
        Expr.Map(run.parameters)
      )

    }

  }

  final case class PullAll() extends ProtocolMessage

  object PullAll {

    implicit object PullAllStructure extends Structured[PullAll] {

      def fromStructure(structure: Structure)(implicit idp: QuineIdProvider): PullAll = {
        assert(structure.signature == signature, "Wrong signature for PULL_ALL")
        structure.fields match {
          case Nil => PullAll()

          case _ =>
            throw new IllegalArgumentException(
              s"Structure with signature of PULL_ALL has the wrong schema"
            )
        }
      }

      val signature: Byte = 0x3F.toByte

      def fields(pullAll: PullAll)(implicit idp: QuineIdProvider): List[Value] = Nil

    }

  }

  final case class DiscardAll() extends ProtocolMessage

  object DiscardAll {

    implicit object DiscardAllStructure extends Structured[DiscardAll] {

      def fromStructure(structure: Structure)(implicit idp: QuineIdProvider): DiscardAll = {
        assert(structure.signature == signature, "Wrong signature for DISCARD_ALL")
        structure.fields match {
          case Nil => DiscardAll()

          case _ =>
            throw new IllegalArgumentException(
              s"Structure with signature of DISCARD_ALL has the wrong schema"
            )
        }
      }

      val signature: Byte = 0x2F.toByte

      def fields(discardAll: DiscardAll)(implicit idp: QuineIdProvider): List[Value] = Nil

    }

  }

  final case class Record(fields: Vector[Value]) extends ProtocolMessage

  object Record {

    implicit object RecordStructure extends Structured[Record] {

      def fromStructure(structure: Structure)(implicit idp: QuineIdProvider): Record = {
        assert(structure.signature == signature, "Wrong signature for RECORD")
        structure.fields match {
          case Expr.List(values) :: Nil => Record(values)

          case _ =>
            throw new IllegalArgumentException(
              s"Structure with signature of RECORD has the wrong schema"
            )
        }
      }

      val signature: Byte = 0x71.toByte

      def fields(record: Record)(implicit idp: QuineIdProvider): List[Value] = List(
        Expr.List(record.fields)
      )

    }

  }

  implicit class ByteStringHexString(bs: ByteString) {

    protected[bolt] def printable: ByteStringHexString = this

    def toHexString: String = bs.map("%02x".format(_)).mkString(" ")

    protected[bolt] def toPrettyString: String = "{" + toHexString + "}"

    override def toString: String = toPrettyString
  }

  // ... TODO other messages
}

final case class BoltSerializationException(
  message: String,
  cause: Throwable = null
) extends RuntimeException(message, cause)
