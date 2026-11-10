package com.thatdot.quine.app.config

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.persistor.PersistenceConfig

/** Cassandra OAuth (`oauth`) is only available in Enterprise, so Quine refuses a Cassandra config that sets it
  * instead of silently connecting without it.
  */
class CassandraOAuthRefusalTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  implicit private val system: ActorSystem = ActorSystem("CassandraOAuthRefusalTest")
  implicit private val logConfig: LogConfig = LogConfig()

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private val oauth = PersistenceAgentType.OAuth2Config(
    clientId = "quine",
    certFile = "/certs/client.p12",
    certAlias = None,
    certFilePassword = "keystore".toCharArray,
    keyAlias = None,
    adfsEnv = None,
    resourceURI = Some("api://cassandra"),
    discoveryURL = Some("https://idp.example.com/.well-known/openid-configuration"),
  )

  test("Quine refuses a Cassandra store that sets oauth, before connecting") {
    val store = PersistenceAgentType.Cassandra(oauth = Some(oauth))
    val refusal = the[IllegalArgumentException] thrownBy
      QuinePersistenceBuilder.instance.build(store, PersistenceConfig())
    refusal.getMessage should include("Cassandra OAuth (`oauth`) is not available")
  }
}
