package org.spark.anonymizer.test

import org.scalatest.{FlatSpec, BeforeAndAfterAll}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec}
import org.spark.anonymizer.DataFrame.Extensions
import org.spark.anonymizer.WebNameDatabase

// scalastyle:off null
class WebNameDatabaseTest extends FlatSpec with BeforeAndAfterAll {
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

  val nameDatabase = new WebNameDatabase()

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
      .convertFirstName(nameDatabase, p => p == "firstname")
      .convertLastName(nameDatabase, p => p == "lastname")
      .convertFullName(nameDatabase, p => p == "fullname")

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
