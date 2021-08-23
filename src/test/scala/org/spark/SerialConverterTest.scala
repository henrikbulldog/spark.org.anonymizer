package org.spark.anonymizer.test

import org.scalatest.{FlatSpec, BeforeAndAfterAll}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec}
import org.spark.anonymizer.DataFrame.Extensions
import org.spark.anonymizer.StringNameDatabase

// scalastyle:off null
class SerialConverterTest extends FlatSpec with BeforeAndAfterAll {
  var spark: SparkSession = _

  override protected def beforeAll() {
    spark = SparkSession.builder
      .appName("AnonymizerTest")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  override protected def afterAll() {
    spark.close()
  }

  val firstNames = Seq("Firstname")
  val lastNames = Seq("Surname")
  val nameDatabase = new StringNameDatabase(Some(firstNames), Some(lastNames))

  "Name conversion" should "be possible" in {
    val sc = spark
    import sc.implicits._

    var df = Seq((1, "Henrik", "Thomsen", "Henrik Thomsen")).toDF(
      "id",
      "firstname",
      "lastname",
      "fullname"
    )

    val convertedDf = df
      .convertFirstName(nameDatabase, p => p == "firstname", Option(100))
      .convertLastName(nameDatabase, p => p == "lastname", Option(100))
      .convertFullName(nameDatabase, p => p == "fullname", Option(100), Option(100))

    df.show(false)
    convertedDf.show(false)

    var shouldBeEmptyDf = df
      .as("o")
      .join(convertedDf.as("c"), $"o.id" === $"c.id")
      .where("o.firstName = c.firstName or o.lastName = c.lastName or o.fullName = c.fullName")
    assert(shouldBeEmptyDf.count == 0, "Expected all columns to be changed")

    shouldBeEmptyDf = convertedDf
    .where("concat(firstName, ' ', lastName) <> fullName")
    assert(shouldBeEmptyDf.count == 0, "Expected firstname + lastname = fullname")
  }
}
// scalastyle:on null
