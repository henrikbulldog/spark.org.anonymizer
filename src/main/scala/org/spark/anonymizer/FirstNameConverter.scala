package org.spark.anonymizer

class FirstNameConverter(nameDatabase: NameDatabase, serialRange: Integer = 0)
    extends NameConverter(nameDatabase) {

  override def getName(seed: Option[Integer] = None): Option[String] = {
    var random = new scala.util.Random
    seed match {
      case None => random = new scala.util.Random
      case _ => random = new scala.util.Random(seed.get)
    }
    val firstNames = nameDatabase.getFirstNames()
    val firstName = firstNames(random.nextInt.abs % firstNames.size)
    if (serialRange > 0) {
      Some(s"${firstName} ${random.nextInt.abs % serialRange}")
    } else {
      Some(firstName)
    }
  }
}
