package org.spark.anonymizer

class FullNameConverter extends NameConverter {

  override def getName(seed: Option[Integer] = None): Option[String] = {
    val firstName = new FirstNameConverter().getName(seed).get
    val lastName = new LastNameConverter().getName(seed).get
    Some(s"$firstName $lastName")
  }

  override def convertName(s: Option[String]): Option[String] = {
    s match {
       case None => None
       case _ =>
        val seed = scala.util.hashing.MurmurHash3.stringHash(s.get).abs
        val firstName = s.get.split(" ").head
        val lastName = s.get.split(" ").tail.mkString(" ")
        val convertedFirstName = new FirstNameConverter().convertName(Some(firstName)).get
        val convertedLastName = new LastNameConverter().convertName(Some(lastName)).get
        Some(s"$convertedFirstName $convertedLastName")
      }
  }

}
