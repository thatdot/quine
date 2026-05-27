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

    val oauthBearerAssertionLogin: Gen[OAuthBearerAssertionLogin] = for {
      clientId <- nonEmptyAlphaNumStr
      certFile <- nonEmptyAlphaNumStr.map(s => s"/etc/quine/$s.jks")
      certFilePassword <- secret
      certFileType <- Gen.option(Gen.oneOf("JKS", "PKCS12"))
      certAlias <- Gen.option(nonEmptyAlphaNumStr)
      keyAlias <- Gen.option(nonEmptyAlphaNumStr)
      resourceHost <- nonEmptyAlphaNumStr
      discoveryHost <- nonEmptyAlphaNumStr
      caCertPath <- Gen.option(nonEmptyAlphaNumStr.map(s => s"/etc/quine/$s-truststore.jks"))
      caCertPassword <- Gen.option(secret)
    } yield OAuthBearerAssertionLogin(
      clientId = clientId,
      certFile = certFile,
      certFilePassword = certFilePassword,
      certFileType = certFileType,
      certAlias = certAlias,
      keyAlias = keyAlias,
      resourceUri = s"https://$resourceHost.example.com/api",
      discoveryUrl = s"https://$discoveryHost.example.com/adfs/.well-known/openid-configuration",
      caCertPath = caCertPath,
      caCertPassword = caCertPassword,
    )

    val saslJaasConfig: Gen[SaslJaasConfig] =
      Gen.oneOf(plainLogin, scramLogin, oauthBearerLogin, oauthBearerAssertionLogin)

    val optSaslJaasConfig: Gen[Option[SaslJaasConfig]] = Gen.option(saslJaasConfig)
  }

  object Arbs {
    implicit val arbSecret: Arbitrary[Secret] = Arbitrary(Gens.secret)
    implicit val arbOptSecret: Arbitrary[Option[Secret]] = Arbitrary(Gens.optSecret)
    implicit val arbPlainLogin: Arbitrary[PlainLogin] = Arbitrary(Gens.plainLogin)
    implicit val arbScramLogin: Arbitrary[ScramLogin] = Arbitrary(Gens.scramLogin)
    implicit val arbOAuthBearerLogin: Arbitrary[OAuthBearerLogin] = Arbitrary(Gens.oauthBearerLogin)
    implicit val arbOAuthBearerAssertionLogin: Arbitrary[OAuthBearerAssertionLogin] =
      Arbitrary(Gens.oauthBearerAssertionLogin)
    implicit val arbSaslJaasConfig: Arbitrary[SaslJaasConfig] = Arbitrary(Gens.saslJaasConfig)
    implicit val arbOptSaslJaasConfig: Arbitrary[Option[SaslJaasConfig]] = Arbitrary(Gens.optSaslJaasConfig)
  }
}
