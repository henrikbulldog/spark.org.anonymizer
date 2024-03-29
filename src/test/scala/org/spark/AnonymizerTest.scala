package org.spark.anonymizer.test

import org.scalatest.{FlatSpec}

import java.sql.{Date, Timestamp}
import java.time.{LocalDateTime}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, BeforeAndAfterAll}
import org.spark.anonymizer.Anonymizer
import org.spark.anonymizer.DataFrame.Extensions

// scalastyle:off null
class AnonymizerTest extends FlatSpec with BeforeAndAfterAll {
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

  "Anonymizing a string" should "be format presreving" in {
    var s = ""
    for (c <- 'a' to 'z') {
      s = s + c
    }
    var a = Anonymizer.anonymizeString(Option(s)).get
    assert(a.length == s.length)
    assert(a.matches("^[a-z]+$"))

    s = ""
    for (c <- 'A' to 'Z') {
      s = s + c
    }
    a = Anonymizer.anonymizeString(Option(s)).get
    assert(a.length == s.length)
    assert(a.matches("^[A-Z]+$"))

    s = ""
    for (c <- '0' to '9') {
      s = s + c
    }
    a = Anonymizer.anonymizeString(Option(s)).get
    assert(a.length == s.length)
    assert(a.matches("^[0-9]+$"))

    s = " -_.:,;@$"
    a = Anonymizer.anonymizeString(Option(s)).get
    assert(a == s)
  }

  "Anonymizing all simple data types" should "be supported" in {
    val sc = spark
    import sc.implicits._

    var original_df = Seq(
      Types(
        1,
        "abcABCæøåÆØÅ .@ 123 1234567890",
        1234567890,
        -123456,
        255,
        123,
        -123.45.toFloat,
        0.0000000000000000000000123,
        Decimal(123.45),
        Timestamp.valueOf(LocalDateTime.now),
        new Date(System.currentTimeMillis())
      ),
      Types(
        1,
        "",
        0,
        0,
        0,
        0,
        0.0.toFloat,
        0.0,
        Decimal(0.0),
        null,
        null
      )
    ).toDF()

    val anonymized_df = original_df.anonymize((p => p != "id"))

    val should_be_empty_df = anonymized_df
      .as("a")
      .join(original_df.as("o"), Seq("id"))
      .filter($"a.s" === $"o.s")
      .filter($"a.l" === $"o.l")
      .filter($"a.i" === $"o.i")
      .filter($"a.sh" === $"o.sh")
      .filter($"a.b" === $"o.b")
      .filter($"a.f" === $"o.f")
      .filter($"a.d" === $"o.d")
      .filter($"a.dec" === $"o.dec")
      .filter($"a.ts" === $"o.ts")
      .filter($"a.dt" === $"o.dt")

    assert(should_be_empty_df.count == 0)
  }

  "Same input" should "yield same output" in {
    val sc = spark
    import sc.implicits._

    var original_df = Seq(
      Types(
        1,
        "abcABCæøåÆØÅ .@ 123 1234567890",
        1234567890,
        -123456,
        255,
        123,
        -123.45.toFloat,
        0.0000000000000000000000123,
        Decimal(123.45),
        Timestamp.valueOf(LocalDateTime.now),
        new Date(System.currentTimeMillis())
      )
    ).toDF()

    var anonymized_1_df = original_df.anonymize((p => p != "id"))
    anonymized_1_df = anonymized_1_df
      .withColumn("hash", hash(anonymized_1_df.columns.map(col): _*))
    var anonymized_2_df = original_df.anonymize((p => p != "id"))
    anonymized_2_df = anonymized_2_df
      .withColumn("hash", hash(anonymized_2_df.columns.map(col): _*))

    val should_be_empty_df = anonymized_1_df
      .as("a")
      .join(anonymized_2_df.as("o"), Seq("id"))
      .filter($"a.hash" =!= $"o.hash")

    assert(should_be_empty_df.count == 0)
  }

  def createCustomers(): Seq[Customer] = {
    val order1 = Order(
      "Product 1",
      123,
      123.45,
      true,
      Array(
        OrderHistory("created"),
        OrderHistory("modified")
      )
    )

    val order2 = Order(
      "Product 2",
      123456,
      12345.78,
      false,
      Array(
        OrderHistory("created")
      )
    )

    Seq(
      Customer(
        1000000001,
        Personal(
          "John Johnson",
          "john.johnson1234@mail.com"
        ),
        Timestamp.valueOf(LocalDateTime.now),
        Array(order1, order2),
        Map(1 -> Category(1, "Cat 1"), 2-> Category(2, "Cat 2"))
      ),
      Customer(
        1000000002,
        null,
        Timestamp.valueOf(LocalDateTime.now),
        null,
        null
      )
    )
  }

  "Anonymizing complex data types" should "be supported" in {
    val sc = spark
    import sc.implicits._

    var original_df = createCustomers.toDF

    val anonymized_df = original_df.anonymize((p => p != "id"))

    original_df.show(false)
    anonymized_df.show(false)
    
    assert(original_df.count == anonymized_df.count)

    val should_be_empty_df = anonymized_df
      .as("a")
      .join(original_df.as("o"), Seq("id"))
      .filter($"a.personal" === $"o.personal"
        || $"a.created" === $"o.created"
        || $"a.orders" === $"o.orders"
        || $"a.orders.qty" === $"o.orders.qty"
        || $"a.orders.price" === $"o.orders.price"
        || $"a.mapping" == $"o.mapping")

    assert(should_be_empty_df.count == 0)    
  }


}
// scalastyle:on null

case class Customer(id: Long, personal: Personal, created: Timestamp, orders: Array[Order], mapping: Map[Int, Category])
case class Personal(name: String, email: String)
case class Order(product: String, qty: Integer, price: Double, inStock: Boolean, history: Array[OrderHistory])
case class OrderHistory(action: String)
case class Category(id: Long, name: String)
case class Types(
    id: Integer,
    s: String,
    l: Long,
    i: Integer,
    sh: Short,
    b: Byte,
    f: Float,
    d: Double,
    dec: Decimal,
    ts: Timestamp,
    dt: Date
)
