package com.thatdot.quine.graph

import scala.reflect.ClassTag

import org.scalacheck.rng.Seed
import org.scalacheck.{Arbitrary, Gen}

object TestDataFactory {

  /** Generate an array of the specified size generating values of a certain size using the
    * generator
    *
    * @param n    length of output array
    * @param size size passed to the generator
    * @param seed used by the generator. DO NOT generate random test input outside of ScalaCheck
    * @param arb  generator
    */
  def generateN[A: ClassTag](n: Int, size: Int, seed: Seed = Seed(1L))(implicit arb: Arbitrary[A]): Array[A] = {
    val output = new Array[A](n)
    val gen: Gen[A] = arb.arbitrary
    val params: Gen.Parameters = Gen.Parameters.default.withSize(size)

    var i = 0
    var nextSeed = seed
    while (i < n) {
      val genRes = gen.doPureApply(params, nextSeed)
      output(i) = genRes.retrieve.get
      i += 1
      nextSeed = genRes.seed
    }

    output
  }

  def generate1[A: ClassTag](size: Int, seed: Seed)(implicit arb: Arbitrary[A]): A =
    generateN(n = 1, size = size, seed = seed).head
}
