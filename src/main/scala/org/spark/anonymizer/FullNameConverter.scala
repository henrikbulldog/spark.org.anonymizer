package org.spark.anonymizer

class FullNameConverter(
    nameDatabase: NameDatabase,
    firstSerialRange: Option[Integer] = None,
    lastSerialRange: Option[Integer] = None
) extends NameConverter
    with Serializable {

  override def convertName(s: Option[String]): Option[String] = {
    s match {
      case None => None
      case _ =>
        if (s.get.trim == "") {
          s
        } else {
          val firstName = s.get.split(" ").head
          val lastName = s.get.split(" ").tail.mkString(" ")
          val convertedFirstName = getName(
            nameDatabase.getFirstNames(),
            firstSerialRange,
            Some(scala.util.hashing.MurmurHash3.stringHash(firstName).abs)
          ).get
          val convertedLastName = getName(
            nameDatabase.getLastNames(),
            lastSerialRange,
            Some(scala.util.hashing.MurmurHash3.stringHash(lastName).abs)
          ).get
          Some(s"$convertedFirstName $convertedLastName")
        }
    }
  }
}
