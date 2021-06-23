package org.spark.anonymizer

trait NameDatabase {
  def getFirstNames(locale: String = "us"): Seq[String]
  def getLastNames(locale: String = "us"): Seq[String]
}
