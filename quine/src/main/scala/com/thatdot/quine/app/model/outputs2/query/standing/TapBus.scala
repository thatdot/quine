package com.thatdot.quine.app.model.outputs2.query.standing

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.concurrent.TrieMap

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.stream.scaladsl.{BroadcastHub, Keep, Source}
import org.apache.pekko.stream.{Materializer, OverflowStrategy}

import sttp.ws.WebSocketFrame

import com.thatdot.data.{DataFoldableFrom, DataFolderTo}
import com.thatdot.quine.graph.NamespaceId

sealed trait SqTapStage {
  def key: String
}
object SqTapStage {
  case object Raw extends SqTapStage { val key = "raw" }
  case object PreEnrichment extends SqTapStage { val key = "pre-enrichment" }
  case object PostEnrichment extends SqTapStage { val key = "post-enrichment" }
}

/** Abstraction over the transport used to fan out tap messages to WebSocket subscribers.
  *
  * Callers use [[TapBus.topicForSq]] to construct a topic key, then check [[hasSubscribers]]
  * before calling [[publish]], and use [[subscriberSource]] from the WebSocket endpoint handler.
  * Implementations are free to route locally (single-node) or across a cluster.
  */
trait TapBus {

  /** Returns true if there is at least one active subscriber for `topic`.
    * Must be cheap enough to call on every stream element (lock-free read).
    */
  def hasSubscribers(topic: String): Boolean

  /** Serialize and broadcast `value` to all current subscribers of `topic`.
    * Must be non-blocking and fire-and-forget.
    * Only called after a [[hasSubscribers]] check — no need to re-guard internally.
    */
  def publish[A](topic: String, value: A)(implicit foldableFrom: DataFoldableFrom[A]): Unit

  /** Return a Source that emits one [[WebSocketFrame.Text]] per published message for `topic`.
    * The source must stay open until the WebSocket disconnects. Registering and unregistering
    * the subscriber is the implementation's responsibility.
    */
  def subscriberSource(topic: String)(implicit mat: Materializer): Source[WebSocketFrame, NotUsed]
}

object TapBus {
  def topicForSq(namespaceId: NamespaceId, sqName: String, outputName: String, stage: SqTapStage): String =
    s"sq-tap/${namespaceId.name}/$sqName/$outputName/${stage.key}"
}

/** Single-node implementation. One `BroadcastHub` per topic, fed by an actor-ref source.
  * A permanent `Sink.ignore` consumer is attached at topic creation so the hub's ring
  * buffer is always being drained and producers are never backpressured by zero consumers.
  * Works with any Pekko actor provider (including `local`).
  */
class LocalTapBus extends TapBus {

  private case class TopicHub(ref: ActorRef, broadcast: Source[String, NotUsed], realSubscribers: AtomicInteger)

  private val hubs = TrieMap.empty[String, TopicHub]

  private def hubFor(topic: String)(implicit mat: Materializer): TopicHub =
    hubs.getOrElseUpdate(
      topic, {
        val (ref, broadcast) = Source
          .actorRef[String](
            completionMatcher = PartialFunction.empty,
            failureMatcher = PartialFunction.empty,
            bufferSize = 256,
            overflowStrategy = OverflowStrategy.dropHead,
          )
          .toMat(BroadcastHub.sink[String](bufferSize = 256))(Keep.both)
          .run()
        broadcast.runWith(org.apache.pekko.stream.scaladsl.Sink.ignore)
        TopicHub(ref, broadcast, new AtomicInteger(0))
      },
    )

  override def hasSubscribers(topic: String): Boolean =
    hubs.get(topic).exists(_.realSubscribers.get() > 0)

  override def publish[A](topic: String, value: A)(implicit foldableFrom: DataFoldableFrom[A]): Unit =
    hubs.get(topic).foreach { hub =>
      val json = foldableFrom.fold(value, DataFolderTo.jsonFolder).noSpaces
      hub.ref ! json
    }

  override def subscriberSource(topic: String)(implicit mat: Materializer): Source[WebSocketFrame, NotUsed] = {
    val hub = hubFor(topic)
    hub.realSubscribers.incrementAndGet()
    hub.broadcast
      .watchTermination() { (_, done) =>
        done.onComplete { _ =>
          hub.realSubscribers.decrementAndGet()
          hubs.updateWith(topic) {
            case Some(h) if h.realSubscribers.get() == 0 => None
            case other => other
          }
        }(mat.executionContext)
        NotUsed
      }
      .map(text => WebSocketFrame.Text(text, finalFragment = true, rsv = None))
  }
}
