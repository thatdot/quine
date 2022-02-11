package com.thatdot.connect

import java.io.FileInputStream
import java.security.KeyStore
import javax.net.ssl.{KeyManagerFactory, SSLContext, SSLEngine, TrustManagerFactory}

import akka.NotUsed
import akka.stream.scaladsl.{BidiFlow, Flow, Source, TLS}
import akka.stream.{IgnoreComplete, TLSProtocol}
import akka.util.ByteString

import com.typesafe.scalalogging.LazyLogging

/** Level of encryption required by a protocol */
sealed abstract class EncryptionLevel {
  def supportsEncrypted: Boolean

  def supportsUnEncrypted: Boolean
}
object EncryptionLevel {
  case object Disabled extends EncryptionLevel {
    def supportsEncrypted = false
    def supportsUnEncrypted = true
  }
  case object Required extends EncryptionLevel {
    def supportsEncrypted = true
    def supportsUnEncrypted = false
  }
  case object Optional extends EncryptionLevel {
    def supportsEncrypted = true
    def supportsUnEncrypted = true
  }
}

object EncryptionUtils extends LazyLogging {

  /** Construct an `SSLEngine` by loading in a keystore
    *
    * One way to make a keystore is using `keytool`:
    *
    * {{{
    * $ keytool -genkeypair -keystore 'my_keystore.keystore'
    * ...
    * }}}
    *
    * @note both `keyStore` and `password` can be `null`, but that will produce
    * an `SSLEngine` that is pretty useless.
    *
    * @param keyStore path to the keystore file
    * @param password password neededto open the keystore file
    */
  private def sslEngine(keyStore: Option[String], password: Option[String]): SSLEngine = {
    val keyInputStream = keyStore.map(new FileInputStream(_)).orNull
    val passwordArr = password.getOrElse("").toCharArray()

    val keystore = KeyStore.getInstance(
      KeyStore.getDefaultType
    )
    keystore.load(keyInputStream, passwordArr)

    val keyManagerFactory = KeyManagerFactory.getInstance(
      KeyManagerFactory.getDefaultAlgorithm
    )
    keyManagerFactory.init(keystore, passwordArr)

    val trustManagerFactory = TrustManagerFactory.getInstance(
      TrustManagerFactory.getDefaultAlgorithm
    )
    trustManagerFactory.init(keystore)

    val sslContext: SSLContext = SSLContext.getInstance("TLSv1.2")
    sslContext.init(
      keyManagerFactory.getKeyManagers,
      trustManagerFactory.getTrustManagers,
      null
    )

    val sslEngine: SSLEngine = sslContext.createSSLEngine()
    sslEngine.setWantClientAuth(true)
    sslEngine.setNeedClientAuth(false)
    sslEngine.setUseClientMode(false)
    sslEngine.setEnableSessionCreation(true)
    sslEngine
  }

  /** TLS bidi that decrypts client messages then encrypts server responses.
    *
    * @param keyStore path to the keystore file
    * @param password password needed to open the keystore file
    */
  def tlsBidi(
    keyStore: Option[String],
    password: Option[String]
  ): BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] =
    TLS
      .apply(
        () => EncryptionUtils.sslEngine(keyStore, password),
        closing = IgnoreComplete
      )
      .reversed
      .atop(
        BidiFlow.fromFlows(
          Flow[TLSProtocol.SslTlsInbound].collect { case sb: TLSProtocol.SessionBytes =>
            sb.bytes // ignore other kinds of inbounds
          },
          Flow[ByteString].map(TLSProtocol.SendBytes)
        )
      )
      .reversed

  /** Depending on the desired encryption level, sets up the right TLS
    * encryption layer around another protocol.
    *
    * TODO: make this a [[BidiFlow]]
    *
    * @param plainProtocol underlying protocol to wrap
    * @param encryption required level of encryption
    * @param keyStore path to the keystore file
    * @param password password needed to open the keystore file
    */
  def possiblyEncrypted(
    plainProtocol: Flow[ByteString, ByteString, NotUsed],
    encryption: EncryptionLevel,
    keyStore: Option[String],
    password: Option[String]
  ): Flow[ByteString, ByteString, NotUsed] = {
    def tlsBidi = EncryptionUtils.tlsBidi(keyStore, password)
    Flow[ByteString]
      .prefixAndTail(1)
      .flatMapConcat { case (Seq(bstr), restBstrs) =>
        val clientSource = Source.single(bstr) ++ restBstrs
        val looksLikeTls: Boolean = bstr.head == 0x16

        // Do our best to warn about confusing connections
        if (looksLikeTls && !encryption.supportsEncrypted) {
          logger.warn(
            "BOLT: client attempted encrypted connection" +
            " (disabled due to `connect.bolt.encryption` setting)"
          )
        }
        if (!looksLikeTls && !encryption.supportsUnEncrypted) {
          logger.warn(
            "BOLT: client attempted unencrypted connection" +
            " (disabled due to `connect.bolt.encryption` setting)"
          )
        }

        // Determine what the encryption layer should be (TLS or pass-through)
        val encryptionLayer: BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] =
          encryption match {
            case EncryptionLevel.Required => tlsBidi
            case EncryptionLevel.Optional if looksLikeTls => tlsBidi
            case EncryptionLevel.Optional => BidiFlow.identity
            case EncryptionLevel.Disabled => BidiFlow.identity
          }

        clientSource.via(plainProtocol.join(encryptionLayer))
      }
  }
}
