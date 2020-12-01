package com.p360.npspend.claims.analytics
import org.apache.spark.internal.Logging
import org.json4s.jackson.JsonMethods.parse


object JsonExtractor extends Logging {

  def extractJson(fileName: String, source: String => String): Map[String, Any] = {
    val receivedJson = source(fileName)
    logDebug(s"Received JSON ${receivedJson}")
    jsonStrToMap(receivedJson)
  }

  private def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    logDebug(s"Reading Json String ${jsonStr}")
    implicit val formats = org.json4s.DefaultFormats
    val map = parse(jsonStr).extract[Map[String, Any]]
    logDebug(s"Reading Json String ${jsonStr}")
    map
  }
}
