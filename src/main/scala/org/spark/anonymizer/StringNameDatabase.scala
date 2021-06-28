package org.spark.anonymizer

class StringNameDatabase(firstNames: Option[Seq[String]], lastNames: Option[Seq[String]]) extends NameDatabase with Serializable {

  override def getFirstNames(): Seq[String] = {
    if (firstNames == None || firstNames.get.size == 0) {
      throw new Exception("First names must be initialized")
    }
    firstNames.get
  }

  override def getLastNames(): Seq[String] = {
    if (lastNames == None || lastNames.get.size == 0) {
      throw new Exception("Last names must be initialized")
    }
    lastNames.get
  }
}
