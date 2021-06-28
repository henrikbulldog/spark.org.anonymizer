package org.spark.anonymizer

class FirstNameConverter(nameDatabase: NameDatabase, serialRange: Option[Integer] = None)
    extends NameConverter
    with Serializable {

  override def convertName(s: Option[String]): Option[String] = {
    s match {
      case None => None
      case _ =>
        if (s.get.trim == "") {
          s
        } else {
          val seed = scala.util.hashing.MurmurHash3.stringHash(s.get).abs
          getName(nameDatabase.getFirstNames(), serialRange, Some(seed))
        }
    }
  }
}
