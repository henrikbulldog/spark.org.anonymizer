package org.spark.anonymizer

class LastNameConverter extends NameConverter {

  override def getName(seed: Option[Integer] = None): Option[String] = {
    var random = new scala.util.Random
    seed match {
       case None => random = new scala.util.Random
       case _ => random = new scala.util.Random(seed.get)
    }
    Some(s"${Names.Nouns(random.nextInt.abs % Names.Nouns.size)} ${random.nextInt.abs % 100}")
  }
}
