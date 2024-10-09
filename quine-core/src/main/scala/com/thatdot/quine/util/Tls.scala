package com.thatdot.quine.util

import nl.altindag.ssl.SSLFactory

object Tls {

  implicit class SSLFactoryBuilderOps(builder: SSLFactory.Builder) {

    /** If the system property `https.cipherSuites` is set, use it to derive the ciphers. Otherwise, return as-is
      * Avoids an IllegalArgumentException from sslcontext-kickstart
      */
    def withSystemPropertyDerivedCiphersSafe(): SSLFactory.Builder = sys.props.get("https.cipherSuites") match {
      case Some(_) => builder.withSystemPropertyDerivedCiphers()
      case _ => builder
    }

    /** If the system property `https.protocols` is set, use it to derive the protocols. Otherwise, return as-is
      * Avoids an IllegalArgumentException from sslcontext-kickstart
      */
    def withSystemPropertyDerivedProtocolsSafe(): SSLFactory.Builder = sys.props.get("https.protocols") match {
      case Some(_) => builder.withSystemPropertyDerivedProtocols()
      case _ => builder
    }
  }
}
