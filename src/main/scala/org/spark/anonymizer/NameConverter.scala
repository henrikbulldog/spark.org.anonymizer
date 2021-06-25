package org.spark.anonymizer

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

abstract class NameConverter(nameDatabase: NameDatabase) extends Serializable {
  def getName(seed: Option[Integer] = None): Option[String]

  def convertName(s: Option[String]): Option[String] = {
    s match {
      case None => None
      case _ =>
        if(s.get.trim == "") {
          s
        } else {
          val seed = scala.util.hashing.MurmurHash3.stringHash(s.get).abs
          getName(Some(seed))
        }
    }
  }

  val convertNameUdf = udf[Option[String], String](s => convertName(Option(s)))

  def convert(df: DataFrame, columnPathFilter: String => Boolean = (p => true)): DataFrame = {
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
          when(c.isNotNull, struct(traverse(s, columnPathFilter, path + f.name + "."): _*))
            .as(f.name)
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
