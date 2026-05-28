package com.thatdot.api.v2

import org.scalacheck.{Arbitrary, Gen}

object ResourceNameGenerators {

  object Gens {

    /** Characters legal in a `ResourceName` per AIP-122 (RFC 3986 unreserved). */
    private val validChar: Gen[Char] =
      Gen.frequency(
        26 -> Gen.alphaUpperChar,
        26 -> Gen.alphaLowerChar,
        10 -> Gen.numChar,
        4 -> Gen.oneOf('_', '-', '.', '~'),
      )

    /** A string drawn from the legal alphabet, bounded to the `ResourceName` length limit. */
    val resourceNameString: Gen[String] = for {
      n <- Gen.chooseNum(1, 128)
      chars <- Gen.listOfN(n, validChar)
    } yield chars.mkString

    /** Valid by construction: the generated string is guaranteed to satisfy `ResourceName.apply`. */
    val resourceName: Gen[ResourceName] = resourceNameString.map { s =>
      ResourceName(s).getOrElse(
        sys.error(
          s"ResourceNameGenerators produced an invalid name: '$s' — generator and validation rule are out of sync",
        ),
      )
    }
  }

  object Arbs {
    implicit val arbResourceName: Arbitrary[ResourceName] = Arbitrary(Gens.resourceName)
  }
}
