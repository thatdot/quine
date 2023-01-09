package com.thatdot.quine.model

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import scala.reflect.ClassTag
import scala.util.{Failure, Try}

import com.typesafe.scalalogging.StrictLogging

import com.thatdot.quine.util.HexConversions

/** Used to map user node IDs to the representation used internally by Quine.
  *
  * Internally, node IDs in Quine are essentially raw byte arrays. This allows
  * Quine to accomodate any ID type: UUIDs, longs, strings, etc. However, in
  * order to use the graph with a certain ID type, Quine needs to know how to
  * turn this type to and from the internal representation. Additionally, mostly
  * for the debugging purposes, the user needs to describe how to print/parse
  * their ID type.
  *
  * In the chart below, solid lines represent total functions and dotted lines
  * represent partial functions.
  *
  * {{{
  *      ,---- customIdToString ----.   ,---- customIdToQid ----. ,- - valueToQid - -.
  *      |                          |   |                       | |                  |
  *      v                          |   |                       v v                  |
  *   String                    CustomIdType                  QuineId          QuineValue
  *      |                          ^ ^ ^                       | |                  ^
  *      |                          | | |                       | |                  |
  *      `- - customIdFromString - -' | `- - customIdFromQid - -' `---- qidToValue --'
  *                                   |
  *                newCustomId -------'
  * }}}
  */
abstract class QuineIdProvider extends StrictLogging {
  type CustomIdType
  val customIdTag: ClassTag[CustomIdType]

  /** Generate a fresh node ID
    *
    * @note MUST be thread safe!
    * @note freshness should be cluster-wide (be wary of determinism)
    */
  def newCustomId(): CustomIdType

  /** Generate a fresh node ID
    *
    * @note IDs produced here have a representation in [[IDType]]
    */
  @inline
  final def newQid(): QuineId = customIdToQid(newCustomId())

  /** Turn a node ID into a string
    *
    * @note this is used mainly for debugging purposes
    * @param typed node ID
    * @return string representation of ID
    */
  def customIdToString(typed: CustomIdType): String

  /** Extract an ID from its string representation
    *
    * @note should be the inverse of [[customIdToString]]
    * @param str string representation of ID
    * @return node ID
    */
  def customIdFromString(str: String): Try[CustomIdType]

  /** Turn a node ID into a Quine-internal raw byte array format
    *
    * @param typed node ID
    * @return raw byte array representation of ID
    */
  def customIdToBytes(typed: CustomIdType): Array[Byte]

  /** Extract an ID from its Quine-internal raw byte array format
    *
    * @note should be the inverse of [[customIdToBytes]]
    * @param bytes raw byte array representation of ID
    * @return node ID
    */
  def customIdFromBytes(bytes: Array[Byte]): Try[CustomIdType]

  final def customIdStringFromQid(qid: QuineId): Try[String] =
    customIdFromQid(qid).map(customIdToString)
  final def customIdStringToQid(s: String): Try[QuineId] = customIdFromString(s).map(customIdToQid)

  final def customIdFromQid(qid: QuineId): Try[CustomIdType] = customIdFromBytes(qid.array)
  final def customIdToQid(typed: CustomIdType): QuineId = QuineId(customIdToBytes(typed))

  /** Similar to [[customIdStringFromQid]], but also handles pretty-printing IDs
    * which have no representation in [[CustomIdType]]. By default:
    *
    *   - if the ID provider understands the ID, use the string representation of the custom ID
    *   - otherwise, use `#{hexadecimal-string}`
    *
    * @param qid ID to pretty-print
    * @return pretty-printed ID
    */
  def qidToPrettyString(qid: QuineId): String =
    customIdFromQid(qid).fold(
      err => {
        val qidStr = "#" + HexConversions.formatHexBinary(qid.array)
        logger.info(s"Failed to serialize QID ${qidStr} with the configured ID provider.", err)
        qidStr
      },
      customIdToString
    )

  /** Inverse of [[qidToPrettyString]]
    *
    * @param str pretty-printed ID
    * @return underlying ID
    */
  final def qidFromPrettyString(str: String): Try[QuineId] =
    customIdStringToQid(str).recoverWith {
      case err if str.head == '#' =>
        Try(QuineId(HexConversions.parseHexBinary(str.tail))) orElse Failure(err)
    }

  /** Extract a Quine-internal ID from a runtime Quine value representation
    *
    * This gets used in query languages when they need to refer to IDs:
    *
    *   - Gremlin: `g.V(1)` to try to convert `1` into an ID
    *   - Cypher: `match (n) where id(n) = 1 return n` to convert `1` into an ID
    *
    * @note should be the inverse of [[qidToValue]]
    * @param value runtime user-representation of the ID
    */
  def valueToQid(value: QuineValue): Option[QuineId] = value match {
    case QuineValue.Id(qid) => Some(qid)
    case _ => None
  }

  /** Convert an ID from its Quine-internal form into a runtime value representation
    *
    * This gets used in query languages when they need to return IDs:
    *
    *   - Gremlin: `g.V(1).id()` to try to convert the ID back to `1`
    *   - Cypher: `match (n) where id(n) = 0 return id(n)` to try to convert the ID back to `1`
    *
    * @note as a default, this can always produce [[QuineValue.Id]]
    * @param qid internal representation of the ID
    */
  def qidToValue(qid: QuineId): QuineValue = QuineValue.Id(qid)

  /** For generating consistent IDs from a starting value.
    * This method should always succeed, regardless of the input.
    * That means it might be lossy or collide; make collisions rare.
    */
  def hashedCustomId(bytes: Array[Byte]): CustomIdType
  def hashedQuineId(bytes: Array[Byte]): QuineId = customIdToQid(hashedCustomId(bytes))
  def hashedQuineId(symbol: Symbol): QuineId =
    customIdToQid(hashedCustomId(symbol.name.getBytes(StandardCharsets.UTF_8)))

  /** Function to determine where in the graph specific nodes live.
    *
    *  @param node any node in graph
    *  @return where in the graph the node lives
    */
  def nodeLocation(node: QuineId): QuineGraphLocation =
    QuineIdProvider.defaultNodeDistribution(node)

}

/** A QuineIdProvider that is "position-aware" by supporting allocation of IDs for a particular position index.
  *
  * Position indices are retrievable from IDs via [[QuineIdProvider.nodeLocation]]. Thus, nodeLocation should be
  * implemented in a manner consistent with [[newCustomIdInNamespace]] and [[hashedCustomIdInNamespace]].
  *
  * Some IDs may leave their position index unspecified, and some IDs that specify a position index may be constructed
  * via means other than this interface.
  */
trait PositionAwareIdProvider extends QuineIdProvider {

  /** Generate a fresh ID corresponding to [[positionIdx]]
    * TODO currently only usable by tests: do we want to support this use case at all?
    */
  def newCustomIdAtPositionIndex(positionIdx: Integer): CustomIdType

  /** Generate a deterministic ID corresponding to [[positionIdx]]
    * INV: given the same [[bytes]] and [[positionIdx]], the same ID must be produced by successive invocations of this
    *      function, including across JVMs
    */
  def hashedCustomIdAtPositionIndex(positionIdx: Integer, bytes: Array[Byte]): CustomIdType
}

object QuineIdProvider {

  type Aux[IdType] = QuineIdProvider { type CustomIdType = IdType }

  /** Turn an array of bytes efficiently into a small fixed-length hash digest
    *
    * @note this should be at least as fast as SHA256
    * @note this may not be a cryptographic hash function (though it currently is)
    * @param bytes the bytes to hash
    * @param length the length the output must have
    * @return hashed array
    */
  def hashToLength(bytes: Array[Byte], length: Int): Array[Byte] = {
    require(length <= 32, "cannot request a digest of length greater than 32!")

    val sha256 = java.security.MessageDigest.getInstance("SHA-256")
    sha256.update(ByteBuffer.allocate(4).putInt(length))
    sha256.digest(bytes).take(length)
  }

  /** Chooses a shard based on a hash of the node ID. Doesn't fix the host. */
  def defaultNodeDistribution(qid: QuineId): QuineGraphLocation = {
    val randomShardIdx = Math.abs(ByteBuffer.wrap(hashToLength(qid.array, 4)).getInt())
    QuineGraphLocation(None, randomShardIdx)
  }
}

/** A location in the cluster
  *
  *   - When the host index is empty, the shard index is modded by the total
  *     number of shards in the logical graph to select a unique shard
  *
  *   - Otherwise, the host index is modded by the total number of hosts to
  *     select a unique host. Then, the shard index is modded by the total
  *     number of shards on that host to select a unique shard.
  *
  * For each, choose whether you want: consistency vs. randomness.
  *
  * @param hostIdx optionally request that the node live on a particular host
  * @param shardIdx the shard index
  */
final case class QuineGraphLocation(hostIdx: Option[Int], shardIdx: Int)
