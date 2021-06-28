package org.spark.anonymizer

trait NameDatabase {
  def getFirstNames(): Seq[String]
  def getLastNames(): Seq[String]
}
