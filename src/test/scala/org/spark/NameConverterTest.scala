package org.spark.anonymizer.test

import org.scalatest.{FlatSpec}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec}
import org.spark.anonymizer.DataFrame.Extensions

object ConversionMethods {
  val Anonymize = "Anonymize"
  val ConvertFirstName = "ConvertFirstName"
  val ConvertLastName = "ConvertLastName"
  val ConvertFullName = "ConvertFullName"
}

case class ColumnConversion (
  method: String,
  columns: Seq[String]
)

case class TableConversion(
    tableName: String,
    columnConversions: Seq[ColumnConversion]
)

// scalastyle:off null
class NameConverterTest extends FlatSpec {
  val spark = SparkSession.builder
    .appName("NameConverterTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  "Name conversion" should "be possible" in {
    var df = Seq((1,"Henrik", "Thomsen", "Henrik Thomsen")).toDF("id", "firstname", "lastname", "fullname")

    val convertedDf = df.convertFirstName((p => p == "firstname"))
      .convertLastName((p => p == "lastname"), 0)
      .convertFullName((p => p == "fullname"), 0, 0)

    df.show(false)
    convertedDf.show(false)

    val shouldBeEmptyDf = df.as("o")
    .join(convertedDf.as("c"), $"o.id" === $"c.id")
    .where("o.firstName = c.firstName or o.lastName = c.lastName or o.fullName = c.fullName")
    assert(shouldBeEmptyDf.count == 0)

  }

  "Name conversion" should "be configurable" in {
    var df = Seq((1,"Henrik", "Thomsen", "Henrik Thomsen", "secret")).toDF("id", "firstname", "lastname", "fullname", "secret")
   
    val config = TableConversion(
      "some table",
      Seq(
        ColumnConversion(ConversionMethods.Anonymize, Seq("secret")), 
        ColumnConversion(ConversionMethods.ConvertFirstName, Seq("firstname")),
        ColumnConversion(ConversionMethods.ConvertLastName, Seq("lastname")),
        ColumnConversion(ConversionMethods.ConvertFullName, Seq("fullname"))
      )
    )

    var convertedDf = convert(df, ConversionMethods.Anonymize, config, ((df, cols) => df.anonymize(p => cols.contains(p))))
    convertedDf = convert(convertedDf, ConversionMethods.ConvertFirstName, config, ((df, cols) => df.convertFirstName(p => cols.contains(p))))
    convertedDf = convert(convertedDf, ConversionMethods.ConvertLastName, config, ((df, cols) => df.convertLastName(p => cols.contains(p))))
    convertedDf = convert(convertedDf, ConversionMethods.ConvertFullName, config, ((df, cols) => df.convertFullName(p => cols.contains(p))))

    df.show(false)
    convertedDf.show(false)

    val shouldBeEmptyDf = df.as("o")
    .join(convertedDf.as("c"), $"o.id" === $"c.id")
    .where("o.firstName = c.firstName or o.lastName = c.lastName or o.fullName = c.fullName or o.secret = c.secret")
    assert(shouldBeEmptyDf.count == 0)
  }

  def convert(
    df: DataFrame, 
    conversionMethod: String,
    config: TableConversion,
    conversion: (DataFrame, Seq[String]) => DataFrame): DataFrame = {
    val columnConversions = config.columnConversions.filter(m => m.method == conversionMethod)
    if (columnConversions.size == 0) {
      df
    } else {
      conversion(df, columnConversions.head.columns)
    }
  }

}
// scalastyle:on null
