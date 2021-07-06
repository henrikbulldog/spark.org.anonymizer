package org.spark.anonymizer

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class NameList(names: Seq[String], serialRange: Option[Integer] = None)
class NameConverter(nameLists: Seq[NameList]) extends Serializable {
  def getName(
      names: Seq[String],
      serialRange: Option[Integer] = None,
      seed: Option[Integer] = None
  ): String = {
    var random = new scala.util.Random
    seed match {
      case None => random = new scala.util.Random
      case _ => random = new scala.util.Random(seed.get)
    }
    val name = names(random.nextInt.abs % names.size)
    serialRange match {
      case None => name
      case _ => s"${name} ${random.nextInt.abs % serialRange.get}"
    }
  }

  def convertName(s: Option[String]): Option[String] = {
    s match {
      case None => None
      case _ =>
        if (s.get.trim == "") {
          s
        } else {
          val parts = s.get.split(" ")
          val convertedParts = nameLists.map(
            e =>
              getName(
                e.names,
                e.serialRange,
                Some(scala.util.hashing.MurmurHash3.stringHash(parts((parts.size-1).min(nameLists.indexOf(e)))))
              )
          )
          Some(convertedParts.mkString(" "))
        }
    }
  }

  val convertNameUdf = udf[Option[String], String](s => convertName(Option(s)))

  def convert(
      df: DataFrame,
      columnPathFilter: String => Boolean = (p => true)
  ): DataFrame = {
    df.select(traverse(df.schema, columnPathFilter): _*)
  }

  // scalastyle:off cyclomatic.complexity
  protected[this] def traverse(
      schema: StructType,
      columnPathFilter: String => Boolean,
      path: String = ""
  ): Array[Column] = {
    schema.fields.map(f => {
      val c = col(path + f.name)
      f.dataType match {
        case s: StructType =>
          when(
            c.isNotNull,
            struct(traverse(s, columnPathFilter, path + f.name + "."): _*)
          ).as(f.name)
        case _ =>
          if (columnPathFilter(path + f.name)) {
            f.dataType match {
              case dt: StringType => convertNameUdf(c).as(f.name)
              case _ => c
            }
          } else {
            c
          }
      }
    })
  }
  // scalastyle:on cyclomatic.complexity
}
