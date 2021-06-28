package org.spark.anonymizer

import scalaj.http.{Http, HttpResponse}
import java.nio.charset.StandardCharsets

class WebNameDatabase(
    firstNamesUrl: String =
      "https://raw.githubusercontent.com/smashew/NameDatabases/master/NamesDatabases/first%20names/us.txt",
    lastNamesUrl: String =
      "https://raw.githubusercontent.com/smashew/NameDatabases/master/NamesDatabases/surnames/us.txt"
) extends NameDatabase
    with Serializable {

  protected var firstNamesCache: Option[Seq[String]] = None
  protected var lastNamesCache: Option[Seq[String]] = None

  override def getFirstNames(): Seq[String] = {
    if (firstNamesCache == None) {
      val rawString = sendHttpGetRequest(firstNamesUrl)
      val bytes = rawString.getBytes(StandardCharsets.US_ASCII)
      firstNamesCache = Some(
        new String(bytes, StandardCharsets.UTF_8)
          .split("\n")
          .toSeq
          .map(_.trim)
          .filter(_ != "")
      )
    }
    firstNamesCache.get
  }

  override def getLastNames(): Seq[String] = {
    if (lastNamesCache == None) {
      val rawString = sendHttpGetRequest(lastNamesUrl)
      val bytes = rawString.getBytes(StandardCharsets.US_ASCII)
      lastNamesCache = Some(
        new String(bytes, StandardCharsets.UTF_8)
          .split("\n")
          .toSeq
          .map(_.trim)
          .filter(_ != "")
      )
    }
    lastNamesCache.get
  }

  protected def sendHttpGetRequest(request: String): String = {
    val httpResponse: HttpResponse[String] = Http(request).asString
    if (httpResponse.code == 200) {
      httpResponse.body
    } else {
      throw new Exception(
        s"Error getting name file: HTTP code ${httpResponse.code}. ${httpResponse.body}"
      )
    }
  }
}
