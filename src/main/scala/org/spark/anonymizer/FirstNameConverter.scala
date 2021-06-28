package org.spark.anonymizer

class FirstNameConverter(nameDatabase: NameDatabase, serialRange: Option[Integer] = None)
    extends NameConverter(nameDatabase) with Serializable {

  override def getName(seed: Option[Integer] = None): Option[String] = {
    var random = new scala.util.Random
    seed match {
      case None => random = new scala.util.Random
      case _ => random = new scala.util.Random(seed.get)
    }
    val firstNames = nameDatabase.getFirstNames()
    val firstName = firstNames(random.nextInt.abs % firstNames.size)
    serialRange match {
      case None => Some(firstName)
      case _ => Some(s"${firstName} ${random.nextInt.abs % serialRange.get}")
    }
  }
}
