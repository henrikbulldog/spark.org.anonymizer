package org.spark.anonymizer.test

import org.scalatest.{FlatSpec}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec}
import org.spark.anonymizer.DataFrame.Extensions
import org.spark.anonymizer.StringNameDatabase
import scala.io.Source

object ConversionMethods {
  val Anonymize = "Anonymize"
  val ConvertFirstName = "ConvertFirstName"
  val ConvertLastName = "ConvertLastName"
  val ConvertFullName = "ConvertFullName"
}

case class ColumnConversion(
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

  val firstNames = Source.fromFile("src/test/scala/org/spark/data/firstNames.txt").getLines.toSeq
  val lastNames = Source.fromFile("src/test/scala/org/spark/data/lastNames.txt").getLines.toSeq
  val nameDatabase = new StringNameDatabase(Some(firstNames), Some(lastNames))

  "Name conversion" should "be possible" in {
    var df = Seq((1, "Henrik", "Thomsen", "Henrik Thomsen")).toDF(
      "id",
      "firstname",
      "lastname",
      "fullname"
    )

    val convertedDf = df
      .convertFirstName(nameDatabase, p => p == "firstname")
      .convertLastName(nameDatabase, p => p == "lastname")
      .convertFullName(nameDatabase, p => p == "fullname")

    df.show(false)
    convertedDf.show(false)

    val shouldBeEmptyDf = df
      .as("o")
      .join(convertedDf.as("c"), $"o.id" === $"c.id")
      .where("o.firstName = c.firstName or o.lastName = c.lastName or o.fullName = c.fullName")
    assert(shouldBeEmptyDf.count == 0)
  }

  "Name conversion" should "deal with empty data" in {
    var df = Seq((1, null, "", " ")).toDF(
      "id",
      "firstname",
      "lastname",
      "fullname"
    )

    val convertedDf = df
      .convertFirstName(nameDatabase, p => p == "firstname")
      .convertLastName(nameDatabase, p => p == "lastname")
      .convertFullName(nameDatabase, p => p == "fullname")

    df.show(false)
    convertedDf.show(false)

    val shouldBeEmptyDf = df
      .as("o")
      .join(convertedDf.as("c"), $"o.id" === $"c.id")
      .where("o.firstName = c.firstName or o.lastName = c.lastName or o.fullName = c.fullName")
    
    val expectedFirstName = null
    val actualFirstName = convertedDf.head().getString(1)
    assert(expectedFirstName == actualFirstName, s"Expected $expectedFirstName, got $actualFirstName")
    
    val expectedLastName = ""
    val actualLastName = convertedDf.head().getString(2)
    assert(expectedLastName == actualLastName, s"Expected $expectedLastName, got $actualLastName")
    
    val expectedFullName = " "
    val actualFullName = convertedDf.head().getString(3)
    assert(expectedFullName == actualFullName, s"Expected $expectedFullName, got $actualFullName")
  }

  "Name conversion" should "be configurable" in {
    var df = Seq((1, "Henrik", "Thomsen", "Henrik Thomsen", "secret")).toDF(
      "id",
      "firstname",
      "lastname",
      "fullname",
      "secret"
    )

    val config = TableConversion(
      "some table",
      Seq(
        ColumnConversion(ConversionMethods.Anonymize, Seq("secret")),
        ColumnConversion(ConversionMethods.ConvertFirstName, Seq("firstname")),
        ColumnConversion(ConversionMethods.ConvertLastName, Seq("lastname")),
        ColumnConversion(ConversionMethods.ConvertFullName, Seq("fullname"))
      )
    )

    var convertedDf = convert(
      df,
      ConversionMethods.Anonymize,
      config,
      ((df, cols) => df.anonymize(p => cols.contains(p)))
    )
    convertedDf = convert(
      convertedDf,
      ConversionMethods.ConvertFirstName,
      config,
      ((df, cols) => df.convertFirstName(nameDatabase, p => cols.contains(p)))
    )
    convertedDf = convert(
      convertedDf,
      ConversionMethods.ConvertLastName,
      config,
      ((df, cols) => df.convertLastName(nameDatabase, p => cols.contains(p), Some(100)))
    )
    convertedDf = convert(
      convertedDf,
      ConversionMethods.ConvertFullName,
      config,
      ((df, cols) => df.convertFullName(nameDatabase, p => cols.contains(p), None, Some(100)))
    )

    df.show(false)
    convertedDf.show(false)

    val shouldBeEmptyDf = df
      .as("o")
      .join(convertedDf.as("c"), $"o.id" === $"c.id")
      .where(
        "o.firstName = c.firstName or o.lastName = c.lastName or o.fullName = c.fullName or o.secret = c.secret"
      )
    assert(shouldBeEmptyDf.count == 0)
  }

  def convert(
      df: DataFrame,
      conversionMethod: String,
      config: TableConversion,
      conversion: (DataFrame, Seq[String]) => DataFrame
  ): DataFrame = {
    val columnConversions = config.columnConversions.filter(m => m.method == conversionMethod)
    if (columnConversions.size == 0) {
      df
    } else {
      conversion(df, columnConversions.head.columns)
    }
  }

}
// scalastyle:on null
