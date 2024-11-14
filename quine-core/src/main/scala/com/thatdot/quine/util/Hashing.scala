package com.thatdot.quine.util

import java.nio.{BufferUnderflowException, ByteBuffer}
import java.util.UUID

import scala.util.{Failure, Success, Try}

import com.google.common.hash.{Funnel, HashFunction}

import com.thatdot.quine.util.Funnels.syntax._

/** Utilities for hashing values using Guava Funnels
  *
  * @see [[com.thatdot.quine.graph.cypher.Expr.addToHasher]]
  * @see [[com.thatdot.quine.graph.StandingQueryResult.putQuineValue]]
  * @see [[com.thatdot.quine.model.DGNHash.putDomainGraphNode]]
  * @see [[Funnels]]
  */
//noinspection UnstableApiUsage
object Hashing {

  /** Hash a value into a UUID
    *
    * @param value what to hash
    * @param function hash function to use (ex: `Hashing.murmur3_128`)
    * @return UUID hash, or a BufferUnderflowException if the hash function does not produce enough bits
    */
  final def hashToUuid[A: Funnel](function: HashFunction, value: A): Try[UUID] =
    // this would happen on the UUID constructor anyways, so save the work of hashing
    if (function.bits < 128) Failure(new BufferUnderflowException())
    else {
      // Try is intentionally omitted, as there are no expected exceptions after validating the hash function's output
      // bit count. Therefore, if we hit something, it truly is exceptional.
      val hasher = function.newHasher
      val bb = ByteBuffer.wrap(hasher.put(value).hash.asBytes)
      Success(new UUID(bb.getLong(), bb.getLong()))
    }

}
