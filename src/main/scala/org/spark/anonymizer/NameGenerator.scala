package org.spark.anonymizer

object NameGenerator {
  def getName(seed: Option[Integer] = None): Option[String] = {
    var random = new scala.util.Random
    seed match {
       case None => random = new scala.util.Random
       case _ => random = new scala.util.Random(seed.get)
    }
    Some(s"${Names.Nouns(random.nextInt.abs % Names.Nouns.size)} ${Names.Adjectives(random.nextInt.abs % Names.Adjectives.size)} ${random.nextInt.abs % 100}" )
  }

  def convertName(s: Option[String]): Option[String] = {
    s match {
       case None => None
       case _ =>
         val seed = scala.util.hashing.MurmurHash3.stringHash(s.get).abs
         getName(Some(seed))
      }
  }
}
