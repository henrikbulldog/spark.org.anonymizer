package org.spark.anonymizer

class LastNameConverter(nameDatabase: NameDatabase, serialRange: Integer = 100)
    extends NameConverter(nameDatabase) {

  override def getName(seed: Option[Integer] = None): Option[String] = {
    var random = new scala.util.Random
    seed match {
      case None => random = new scala.util.Random
      case _ => random = new scala.util.Random(seed.get)
    }
    val lastNames = nameDatabase.getLastNames()
    val lastName = lastNames(random.nextInt.abs % lastNames.size)
    if (serialRange > 0) {
      Some(s"${lastName} ${random.nextInt.abs % serialRange}")
    } else {
      Some(lastName)
    }
  }
}
