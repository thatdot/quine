package com.thatdot.quine.persistor

import java.nio.charset.StandardCharsets.UTF_8

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.defaultNamespaceId

/** [[PrimePersistor.setMetaDataIfValue]]'s GENERIC implementation: the one every persistor without
  * engine-level support inherits, exercised here through the in-memory persistor.
  *
  * Worth its own suite because that default is where the guarantee comes from for MapDB, RocksDB
  * and in-memory deployments: each owns its store outright, so serializing this process's
  * conditional writes is the whole of the exclusion, and the last test is the one that says the
  * serialization is real. If the chain were a plain `synchronized` (which releases when the Future
  * is returned, not when the write lands), every case here would still pass except that one.
  */
class ConditionalMetaDataWriteTest extends AnyFunSuite {

  import scala.concurrent.ExecutionContext.Implicits.global

  private def bytes(s: String): Array[Byte] = s.getBytes(UTF_8)
  private def await[A](f: Future[A]): A = Await.result(f, 10.seconds)

  private def persistor(): PrimePersistor = {
    val p = InMemoryPersistor.namespacePersistor
    await(p.prepareNamespace(defaultNamespaceId))
    p
  }

  test("claiming an absent key with an absent witness succeeds") {
    val p = persistor()
    assert(await(p.setMetaDataIfValue("k", None, Some(bytes("v1")))) == ConditionalWriteResult.Written)
    assert(await(p.getMetaData("k")).map(new String(_, UTF_8)).contains("v1"))
  }

  test("a second claim of the same key loses, and is told what is there") {
    val p = persistor()
    await(p.setMetaDataIfValue("k", None, Some(bytes("first"))))
    await(p.setMetaDataIfValue("k", None, Some(bytes("second")))) match {
      case ConditionalWriteResult.Conflict(current) =>
        assert(current.map(new String(_, UTF_8)).contains("first"))
      case other => fail(s"expected a conflict, got $other")
    }
    assert(await(p.getMetaData("k")).map(new String(_, UTF_8)).contains("first"), "the loser must not have written")
  }

  test("an update carrying the current value succeeds") {
    val p = persistor()
    await(p.setMetaDataIfValue("k", None, Some(bytes("v1"))))
    assert(await(p.setMetaDataIfValue("k", Some(bytes("v1")), Some(bytes("v2")))) == ConditionalWriteResult.Written)
    assert(await(p.getMetaData("k")).map(new String(_, UTF_8)).contains("v2"))
  }

  test("an update carrying a stale value is refused and reports the winner") {
    val p = persistor()
    await(p.setMetaDataIfValue("k", None, Some(bytes("v1"))))
    await(p.setMetaDataIfValue("k", Some(bytes("v1")), Some(bytes("v2"))))
    // A writer still holding v1: the in-flight write from a superseded incarnation.
    await(p.setMetaDataIfValue("k", Some(bytes("v1")), Some(bytes("clobber")))) match {
      case ConditionalWriteResult.Conflict(current) =>
        assert(current.map(new String(_, UTF_8)).contains("v2"))
      case other => fail(s"expected a conflict, got $other")
    }
    assert(await(p.getMetaData("k")).map(new String(_, UTF_8)).contains("v2"))
  }

  test("a conditional delete carrying the current value removes the key") {
    val p = persistor()
    await(p.setMetaDataIfValue("k", None, Some(bytes("v1"))))
    assert(await(p.setMetaDataIfValue("k", Some(bytes("v1")), None)) == ConditionalWriteResult.Written)
    assert(await(p.getMetaData("k")).isEmpty)
  }

  test("a conditional delete carrying a stale value leaves the key alone") {
    val p = persistor()
    await(p.setMetaDataIfValue("k", None, Some(bytes("v1"))))
    await(p.setMetaDataIfValue("k", Some(bytes("v1")), Some(bytes("v2"))))
    assert(await(p.setMetaDataIfValue("k", Some(bytes("v1")), None)).isInstanceOf[ConditionalWriteResult.Conflict])
    assert(await(p.getMetaData("k")).map(new String(_, UTF_8)).contains("v2"))
  }

  test("concurrent writers from one witness produce exactly one winner") {
    val p = persistor()
    await(p.setMetaDataIfValue("k", None, Some(bytes("base"))))

    // All twenty believe the value is still "base": the shape of several writers acting on the same
    // read. Without serialization their reads interleave with each other's writes and more
    // than one sees "base" still there, so more than one is told it won.
    val results = await(
      Future.traverse(1 to 20)(i => p.setMetaDataIfValue("k", Some(bytes("base")), Some(bytes(s"w$i")))),
    )

    assert(results.count(_ == ConditionalWriteResult.Written) == 1)
    assert(results.count(_.isInstanceOf[ConditionalWriteResult.Conflict]) == 19)

    // And the value stored is the one that was told it won.
    val stored = await(p.getMetaData("k")).map(new String(_, UTF_8))
    assert(stored.exists(_.startsWith("w")))
  }
}
