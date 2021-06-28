package org.spark.anonymizer

class FullNameConverter(
    nameDatabase: NameDatabase,
    firstSerialRange: Option[Integer] = None,
    lastSerialRange: Option[Integer] = None
) extends NameConverter(nameDatabase) with Serializable {

  override def getName(seed: Option[Integer] = None): Option[String] = {
    val firstName = new FirstNameConverter(nameDatabase, firstSerialRange).getName(seed).get
    val lastName = new LastNameConverter(nameDatabase, lastSerialRange).getName(seed).get
    Some(s"$firstName $lastName")
  }

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
          val convertedFirstName =
            new FirstNameConverter(nameDatabase, firstSerialRange).convertName(Some(firstName)).get
          val convertedLastName =
            new LastNameConverter(nameDatabase, lastSerialRange).convertName(Some(lastName)).get
          Some(s"$convertedFirstName $convertedLastName")
        }
    }
  }
}
