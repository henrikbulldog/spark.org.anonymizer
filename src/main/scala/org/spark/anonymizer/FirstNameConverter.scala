package org.spark.anonymizer

class FirstNameConverter extends NameConverter {

  override def getName(seed: Option[Integer] = None): Option[String] = {
    var random = new scala.util.Random
    seed match {
       case None => random = new scala.util.Random
       case _ => random = new scala.util.Random(seed.get)
    }
    Some(Names.Adjectives(random.nextInt.abs % Names.Adjectives.size))
  }
}
