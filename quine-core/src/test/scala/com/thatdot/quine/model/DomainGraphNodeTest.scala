package com.thatdot.quine.model

import scala.collection.mutable

import com.google.common.hash.Hashing.murmur3_128
import org.scalacheck.rng.Seed
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.graph.{ArbitraryInstances, Generators}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.PropertyComparisonFunctions.{
  Identicality,
  ListContains,
  NoValue,
  NonIdenticality,
  Wildcard,
}

class DomainGraphNodeTest extends AnyFlatSpec with Matchers with ArbitraryInstances {
  val seed: Seed = Seed(0)
  it must "generate stable identifiers for arbitrary values" in {
    val hasher = murmur3_128.newHasher
    val input = Generators.generateN[DomainGraphNode](100000, 200, seed)
    for { dgn <- input } hasher.putLong(DomainGraphNode.id(dgn))
    hasher.hash.asLong shouldBe -3231038059776559063L
  }
  it must "generate unique identifiers for arbitrary values" in {
    val nodes = mutable.Set.empty[DomainGraphNode]
    val ids = mutable.Set.empty[DomainGraphNodeId]
    val len = 10000
    val input = Generators.generateN[DomainGraphNode](len, 200, seed)
    for {
      dgn <- input
      if !nodes.contains(dgn)
    } {
      nodes add dgn
      val id = DomainGraphNode.id(dgn)
      assert(!ids.contains(id))
      ids add id
    }
  }
  it must "t1" in {
    assert(
      DomainGraphNode.id(
        DomainGraphNode.Single(
          DomainNodeEquiv(
            None,
            Map(
              (Symbol("EII"), (Identicality, Some(PropertyValue(QuineValue.Null)))),
            ),
            Set.empty,
          ),
          Some(QuineId.fromInternalString("067B6DBEFE32F32C9AED112D995EC159AD2AC6AD038EEE5A")),
          Seq.empty,
          NodeLocalComparisonFunctions.Identicality,
        ),
      ) === 4058705439931334192L,
    )
  }
  it must "t2" in {
    assert(
      DomainGraphNode.id(
        DomainGraphNode.Single(
          DomainNodeEquiv(
            None,
            Map.empty,
            Set.empty,
          ),
          None,
          Seq.empty,
          NodeLocalComparisonFunctions.Wildcard,
        ),
      ) === -1908283053104376279L,
    )
  }
  it must "t3" in {
    val dgn = DomainGraphNode.Single(
      DomainNodeEquiv(
        None,
        Map(
          (Symbol("EII"), (Identicality, Some(PropertyValue(QuineValue.Null)))),
          (Symbol("TYD"), (NoValue, Some(PropertyValue(QuineValue.False)))),
          (Symbol("jQOeau"), (Identicality, Some(PropertyValue(QuineValue.Null)))),
          (Symbol("Fkq"), (Identicality, Some(PropertyValue(QuineValue.True)))),
          (Symbol("jiurqCTYsNnlKcfkZzKsMBItBVHluzyb"), (NoValue, Some(PropertyValue(QuineValue.Null)))),
          (Symbol("DShGE"), (ListContains(Set(QuineValue.Str("KNOWS"))), Some(PropertyValue(QuineValue.Null)))),
          (Symbol("do"), (NonIdenticality, Some(PropertyValue(QuineValue.False)))),
          (Symbol("UA"), (Wildcard, Some(PropertyValue(QuineValue.True)))),
          (Symbol("oOKKigj"), (ListContains(Set(QuineValue.Str("KNOWS"))), Some(PropertyValue(QuineValue.True)))),
        ),
        Set(),
      ),
      None,
      List(
      ),
      NodeLocalComparisonFunctions.Wildcard,
    )
    assert(DomainGraphNode.id(dgn) === -1045660870877700950L, dgn.toString)
  }
  it must "t4" in {
    assert(
      DomainGraphNode.id(
        DomainGraphNode.Single(
          DomainNodeEquiv(
            None,
            Map(
              (Symbol("jiurqCTYsNnlKcfkZzKsMBItBVHluzyb"), (Identicality, Some(PropertyValue(QuineValue.Null)))),
            ),
            Set.empty,
          ),
          Some(QuineId.fromInternalString("067B6DBEFE32F32C9AED112D995EC159AD2AC6AD038EEE5A")),
          Seq.empty,
          NodeLocalComparisonFunctions.Identicality,
        ),
      ) === -1988235072776381205L,
    )
  }
  it must "t5" in {
    assert(
      DomainGraphNode.id(
        DomainGraphNode.Single(
          DomainNodeEquiv(
            None,
            Map(
              (Symbol("jiurqCTYsNnlKcfkZzKsMBItBVHluzyb"), (NoValue, Some(PropertyValue(QuineValue.Null)))),
            ),
            Set.empty,
          ),
          Some(QuineId.fromInternalString("067B6DBEFE32F32C9AED112D995EC159AD2AC6AD038EEE5A")),
          Seq.empty,
          NodeLocalComparisonFunctions.Identicality,
        ),
      ) === 3652871857713815700L,
    )
  }
}
