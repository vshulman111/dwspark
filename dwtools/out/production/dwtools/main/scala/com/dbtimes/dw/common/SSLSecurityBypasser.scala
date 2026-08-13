package com.dbtimes.dw.common

// This code copied from https://github.com/tferega/scala-ssl-disabler/blob/master/scala-ssl-disabler/SecurityBypasser.scala
// Author: Tomo Ferega - tferega
// Copyright (c) 2017 Tomo Ferega

// Usage (Scala)
// Put the SecurityBypasser object in your project. You can then use destroySSLSecurity and restoreSllSecurity methods directly, or you can use the executeBypassed method:
//
// val result = SSLSecurityBypasser.executeBypassed {
// ...
// }
// Usage (Scala)
// Put the SecurityBypasser class in your project. You can then use destroySSLSecurity and restoreSllSecurity methods:
//
// SSLSecurityBypasser.destroySSLSecurity();
// ...
// SSLSecurityBypasser.restoreSllSecurity();

// Just copy-paste this code to wherever, and call
// destroyAllSSLSecurityForTheEntireVMForever method to freely enjoy the wild
// world of invlaid SSL certificates.

object SSLSecurityBypasser {
  import java.security.cert.X509Certificate
  import javax.net.ssl._

  private[this] case class Defaults(sslSocketFactory: SSLSocketFactory, hostnameVerifier: HostnameVerifier)
  private[this] var defaults: Option[Defaults] = None

  // The all-permisive trust manager.
  private[this] object AllTM extends X509TrustManager {
    def getAcceptedIssuers: Array[X509Certificate] = null
    def checkClientTrusted(certs: Array[X509Certificate], authType: String) {}
    def checkServerTrusted(certs: Array[X509Certificate], authType: String) {}
  }

  // The all-permissive hostname verifier.
  private[this] object AllHosts extends HostnameVerifier {
    def verify(urlHostName: String, session: SSLSession) = true
  }

  def destroySSLSecurity(): Unit = {
    // Save the current defaults so that they can later be restored.
    defaults = Some(Defaults(HttpsURLConnection.getDefaultSSLSocketFactory, HttpsURLConnection.getDefaultHostnameVerifier))

    // Create the new, permissive SSL context.
    val trustAllCerts = Array[TrustManager](AllTM)
    val sslContext = SSLContext.getInstance("SSL")
    sslContext.init(null, trustAllCerts, null)

    // Set the connection.
    HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory)
    HttpsURLConnection.setDefaultHostnameVerifier(AllHosts)
  }

  def restoreSSLSecurity(): Unit = {
    // If defaults is set, restore the originals.
    defaults.foreach { d =>
      HttpsURLConnection.setDefaultSSLSocketFactory(d.sslSocketFactory)
      HttpsURLConnection.setDefaultHostnameVerifier(d.hostnameVerifier)
    }
  }

  // Example: executes a single function in a bypassed environment.
  def executeBypassed[T](f: => T): T = {
    destroySSLSecurity()
    var r = f
    restoreSSLSecurity()
    r
  }
}