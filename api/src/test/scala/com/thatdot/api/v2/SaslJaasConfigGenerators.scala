package com.thatdot.api.v2

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.common.security.Secret
import com.thatdot.quine.ScalaPrimitiveGenerators

object SaslJaasConfigGenerators {

  import ScalaPrimitiveGenerators.Gens.nonEmptyAlphaNumStr

  object Gens {

    // This may be worth putting in into a SecretGenerators, but more likely after we pull quine-common into quine-plus
    val secret: Gen[Secret] = nonEmptyAlphaNumStr.map(Secret(_))

    // This may be worth putting in into a SecretGenerators, but more likely after we pull quine-common into quine-plus
    val optSecret: Gen[Option[Secret]] = Gen.option(secret)

    val plainLogin: Gen[PlainLogin] = for {
      username <- nonEmptyAlphaNumStr
      password <- secret
    } yield PlainLogin(username, password)

    val scramLogin: Gen[ScramLogin] = for {
      username <- nonEmptyAlphaNumStr
      password <- secret
    } yield ScramLogin(username, password)

    val oauthBearerLogin: Gen[OAuthBearerLogin] = for {
      clientId <- nonEmptyAlphaNumStr
      clientSecret <- secret
      scope <- Gen.option(nonEmptyAlphaNumStr)
      tokenEndpointUrl <- Gen.option(nonEmptyAlphaNumStr.map(s => s"https://$s.example.com/oauth/token"))
    } yield OAuthBearerLogin(clientId, clientSecret, scope, tokenEndpointUrl)

    val saslJaasConfig: Gen[SaslJaasConfig] =
      Gen.oneOf(plainLogin, scramLogin, oauthBearerLogin)

    val optSaslJaasConfig: Gen[Option[SaslJaasConfig]] = Gen.option(saslJaasConfig)
  }

  object Arbs {
    implicit val arbSecret: Arbitrary[Secret] = Arbitrary(Gens.secret)
    implicit val arbOptSecret: Arbitrary[Option[Secret]] = Arbitrary(Gens.optSecret)
    implicit val arbPlainLogin: Arbitrary[PlainLogin] = Arbitrary(Gens.plainLogin)
    implicit val arbScramLogin: Arbitrary[ScramLogin] = Arbitrary(Gens.scramLogin)
    implicit val arbOAuthBearerLogin: Arbitrary[OAuthBearerLogin] = Arbitrary(Gens.oauthBearerLogin)
    implicit val arbSaslJaasConfig: Arbitrary[SaslJaasConfig] = Arbitrary(Gens.saslJaasConfig)
    implicit val arbOptSaslJaasConfig: Arbitrary[Option[SaslJaasConfig]] = Arbitrary(Gens.optSaslJaasConfig)
  }
}
