package org.spark.anonymizer

class FullNameConverter(
    nameDatabase: NameDatabase,
    firstSerialRange: Option[Integer] = None,
    lastSerialRange: Option[Integer] = None
) extends NameConverter with Serializable {

  override def convertName(s: Option[String]): Option[String] = {
    s match {
      case None => None
      case _ =>
        if (s.get.trim == "") {
          s
        } else {
          val seed = scala.util.hashing.MurmurHash3.stringHash(s.get).abs
          val firstName = s.get.split(" ").head
          val lastName = s.get.split(" ").tail.mkString(" ")
          val convertedFirstName = getName(nameDatabase.getFirstNames(), firstSerialRange, Some(seed)).get
          val convertedLastName = getName(nameDatabase.getLastNames(), lastSerialRange, Some(seed)).get
          Some(s"$convertedFirstName $convertedLastName")
        }
    }
  }
}
