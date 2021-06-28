package org.spark.anonymizer

class LastNameConverter(
    nameDatabase: NameDatabase,
    serialRange: Option[Integer] = None
) extends NameConverter(nameDatabase)
    with Serializable {

  override def getName(seed: Option[Integer] = None): Option[String] = {
    var random = new scala.util.Random
    seed match {
      case None => random = new scala.util.Random
      case _ => random = new scala.util.Random(seed.get)
    }
    val lastNames = nameDatabase.getLastNames()
    val lastName = lastNames(random.nextInt.abs % lastNames.size)
    serialRange match {
      case None => Some(lastName)
      case _ => Some(s"${lastName} ${random.nextInt.abs % serialRange.get}")
    }
  }
}
