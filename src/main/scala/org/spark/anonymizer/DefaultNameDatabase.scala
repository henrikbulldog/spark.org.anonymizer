package org.spark.anonymizer

object DefaultNameDatabase extends NameDatabase with Serializable {

  protected var FirstNamesCache: Seq[String] = Seq()
  protected var LastNamesCache: Seq[String] = Seq()

  override def getFirstNames(): Seq[String] = {
    if (FirstNamesCache.size == 0) {
      throw new Exception("First names must be initialized")
    }
    FirstNamesCache
  }

  override def getLastNames(): Seq[String] = {
    if (LastNamesCache.size == 0) {
      throw new Exception("Last names must be initialized")
    }
    LastNamesCache
  }

  def setFirstNames(firstNames: Seq[String]) {
    FirstNamesCache = firstNames
  }

  def setLastNames(lastNames: Seq[String]) {
    LastNamesCache = lastNames
  }
}
