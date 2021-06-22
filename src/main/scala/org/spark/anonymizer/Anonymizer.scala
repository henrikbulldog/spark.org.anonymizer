package org.spark.anonymizer

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.sql.{Date, Timestamp}
import java.math.BigDecimal
import spray.json._ // See https://javadoc.io/static/io.spray/spray-json_2.12/1.3.5/spray/json/index.html
import scala.math.signum

/**
  * Anonymizes selected columns in a dataframe while preserving format.
  *
  * To anonymize selected columns in a dataframe:
  *
  * import org.spark.Anonymizer.Extensions
  *
  * val df = input_df.anonymize((p => Array("col1", "col2").contains(p)))
  *
  * To anonymize all columns in a dataframe:
  * val df = input_df.anonymize()
  *
  * To anonymize all columns in a dataframe except one:
  * val df = input_df.anonymize((p => p != "id"))
  *
  * To anonymize a single column:
  *
  * import org.spark.Anonymizer.Extensions
  *
  * df.withColumn("anonymized_col1", Anonymizer.AnonymizeStringUdf($"col1"))
  */
object Anonymizer extends Serializable {

  /**
    * Anonymize selected fields in a dataframe.
    *  @param df: DataFrame to update.
    *  @param columnPathFilter: A function to filter what column paths are anonymized.
    *  @return: Anonymized dataframe.
    */
  def anonymize(df: DataFrame, columnPathFilter: String => Boolean = (p => true)): DataFrame = {
    mutate(df, columnPathFilter)
  }

  /**
    * UDF to anonymize a string while preserving its format.
    *  @param s: String to anonymize.
    *  @return: Anonymized string.
    */
  val AnonymizeStringUdf = udf[Option[String], String](s => anonymizeString(Option(s)))

  /**
    * UDF to anonymize a Long while preserving its number of digits.
    *  @param l: Long to anonymize.
    *  @return: Anonymized Long.
    */
  val AnonymizeLongUdf = udf[Option[Long], Long](l => anonymizeLong(Option(l)))

  /**
    * UDF to anonymize an Integer while preserving its number of digits.
    *  @param i: Integer to anonymize.
    *  @return: Anonymized Integer.
    */
  val AnonymizeIntegerUdf = udf[Option[Integer], Integer](i => {
    Option(i) match {
      case None => None
      case _ =>
        val l = anonymizeLong(Option(i.toLong))
        l match {
          case None => None
          case _ => Some(l.get.toInt)
        }
    }
  })

  /**
    * UDF to anonymize an Short while preserving its number of digits.
    *  @param s: Short to anonymize.
    *  @return: Anonymized Short.
    */
  val AnonymizeShortUdf = udf[Option[Short], Short](s => {
    Option(s) match {
      case None => None
      case _ =>
        val l = anonymizeLong(Option(s.toLong))
        l match {
          case None => None
          case _ => Some(l.get.toShort)
        }
    }
  })

  /**
    * UDF to anonymize an Byte while preserving its number of digits.
    *  @param b: Byte to anonymize.
    *  @return: Anonymized Byte.
    */
  val AnonymizeByteUdf = udf[Option[Byte], Byte](b => {
    Option(b) match {
      case None => None
      case _ =>
        val l = anonymizeLong(Option(b.toLong))
        l match {
          case None => None
          case _ => Some(l.get.toByte)
        }
    }
  })

  /**
    * UDF to anonymize a Float.
    *  @param f: Float to anonymize.
    *  @return: Anonymized Float.
    */
  val AnonymizeFloatUdf = udf[Option[Float], Float](f => {
    Option(f) match {
      case None => None
      case _ =>
        val d = anonymizeDouble(Option(f.toDouble))
        d match {
          case None => None
          case _ => Some(d.get.toFloat)
        }
    }
  })

  /**
    * UDF to anonymize a Double.
    *  @param d: Double to anonymize.
    *  @return: Anonymized Double.
    */
  val AnonymizeDoubleUdf = udf[Option[Double], Double](d => anonymizeDouble(Option(d)))

  /**
    * UDF to anonymize a Decimal.
    *  @param dec: Decimal to anonymize.
    *  @return: Anonymized Decimal.
    */
  val AnonymizeDecimalUdf = udf[Option[BigDecimal], BigDecimal](dec => {
    Option(dec) match {
      case None => None
      case _ =>
        val d = anonymizeDouble(Option(dec.doubleValue))
        d match {
          case None => None
          case _ => Some(new BigDecimal(d.get))
        }
    }
  })

  /**
    * UDF to anonymize a Timestamp.
    *  @param ts: Timestamp to anonymize.
    *  @return: Anonymized Timestamp.
    */
  val AnonymizeTimestampUdf =
    udf[Option[Timestamp], Timestamp](ts => anonymizeTimestamp(Option(ts)))

  /**
    * UDF to anonymize a Date.
    *  @param date: Date to anonymize.
    *  @return: Anonymized Date.
    */
  val AnonymizeDateUdf = udf[Option[Date], Date](date => {
    Option(date) match {
      case None => None
      case _ =>
        val dateAsTimeStamp = Option(new Timestamp(date.getTime))
        val anonymized = anonymizeTimestamp(dateAsTimeStamp)
        anonymized match {
          case None => None
          case _ => Some(new Date(anonymized.get.getTime))
        }
    }
  })

  /**
    * UDF to anonymize a JSON string while preserving property names
    * @param s: JSON string
    * @return: Anonymized JSON string
    */
  val AnonymizeJsonStringUdf = udf[Option[String], String](s => anonymizeJsonString(Option(s)))

  protected[this] val AsciiUpperLetters = ('A' to 'Z').toList.filter(_.isLetter)
  protected[this] val AsciiLowerLetters = ('a' to 'z').toList.filter(_.isLetter)
  protected[this] val UtfLetters = (128.toChar to 256.toChar).toList.filter(_.isLetter)
  protected[this] val Numbers = ('0' to '9')

  /**
    * Anonymize a string while preserving its format.
    *  @param s: String to anonymize.
    *  @return: Anonymized string.
    */
  def anonymizeString(s: Option[String]): Option[String] = {
    s match {
      case None => None
      case _ =>
        val seed = scala.util.hashing.MurmurHash3.stringHash(s.get).abs
        val random = new scala.util.Random(seed)
        var r = ""
        for (c <- s.get) {
          if (Numbers.contains(c)) {
            r = r + (((random.nextInt.abs + c) % Numbers.size))
          } else if (AsciiUpperLetters.contains(c)) {
            r = r + AsciiUpperLetters(((random.nextInt.abs) % AsciiUpperLetters.size))
          } else if (AsciiLowerLetters.contains(c)) {
            r = r + AsciiLowerLetters(((random.nextInt.abs) % AsciiLowerLetters.size))
          } else if (UtfLetters.contains(c)) {
            r = r + UtfLetters(((random.nextInt.abs) % UtfLetters.size))
          } else {
            r = r + c
          }
        }
        Some(r)
    }
  }

  val MinNumber = 128

/**
    * Function to anonymize a Long while preserving its number of digits.
    *  @param l: Long to anonymize.
    *  @return: Anonymized Long.
    */
    def anonymizeLong(l: Option[Long]): Option[Long] = {
    l match {
      case None => None
      case _ =>
        val v = l.get
        if (v == 0) {
          0
        }
        val seed = scala.util.hashing.MurmurHash3.stringHash(v.toString).abs
        val random = new scala.util.Random(seed)
        Some(random.nextLong.abs % math.max(v.abs, MinNumber) * signum(v))
    }
  }

  /**
    * Function to anonymize a Double.
    *  @param d: Double to anonymize.
    *  @return: Anonymized Double.
    */
  def anonymizeDouble(d: Option[Double]): Option[Double] = {
    d match {
      case None => None
      case _ =>
        val v = d.get
        if (v == 0) {
          0
        }
        val seed = scala.util.hashing.MurmurHash3.stringHash(v.toString).abs
        val random = new scala.util.Random(seed)
        Some(random.nextDouble.abs % math.max(v.abs, MinNumber) * signum(v))
    }
  }

  /**
    * Function to anonymize a Timestamp.
    *  @param ts: Timestamp to anonymize.
    *  @return: Anonymized Timestamp.
    */
  def anonymizeTimestamp(ts: Option[Timestamp]): Option[Timestamp] = {
    ts match {
      case None => None
      case _ =>
        val v = ts.get
        val seed = scala.util.hashing.MurmurHash3.stringHash(v.toString).abs
        val random = new scala.util.Random(seed)
        val ms = v.getTime()
        Some(new Timestamp((random.nextLong % (ms / 2)) + (ms / 2)))
    }
  }

  /**
    * Function to anonymize a JSON string while preserving property names
    * @param s: JSON string
    * @return: Anonymized JSON string
    */
  def anonymizeJsonString(jsonString: Option[String]): Option[String] = {
    jsonString match {
      case None => None
      case _ =>
        val jsonAst = jsonString.get.parseJson
        val anonymizedJsonAst = anonymizeJsonAst(jsonAst)
        Some(anonymizedJsonAst.compactPrint)
    }
  }

  /**
    * Function to anonymize a JsValue (JSON AST document, see https://javadoc.io/static/io.spray/spray-json_2.12/1.3.5/spray/json/JsValue.html)
    *
    * @param jsValue: JSON nested document
    * @return: Anonymized JSON document
    */
  protected def anonymizeJsonAst(jsValue: JsValue): JsValue = {
    jsValue match {
      case JsArray(_) =>
        val arr = jsValue.asInstanceOf[JsArray]
        val elements = arr.elements
        val anonymizedElements = elements.map(e => anonymizeJsonAst(e))
        JsArray(anonymizedElements)
      case JsObject(_) =>
        val jsObject = jsValue.asInstanceOf[JsObject]
        val fields = jsObject.fields
        val anonymizedFields = fields.map(kv => (kv._1 -> anonymizeJsonAst(kv._2)))
        JsObject(anonymizedFields)
      case JsString(_) =>
        val jsString = jsValue.asInstanceOf[JsString]
        val s = jsString.value
        val anonymizedString = Anonymizer.anonymizeString(Option(s))
        anonymizedString match {
          case None => JsNull
          case _ => JsString(anonymizedString.get)
        }
      case JsNumber(_) =>
        val jsNumber = jsValue.asInstanceOf[JsNumber]
        val s = jsNumber.toString
        val anonymizedString = Anonymizer.anonymizeString(Option(s))
        anonymizedString match {
          case None => JsNull
          case _ => JsNumber(new BigDecimal(anonymizedString.get))
        }
      case _ => jsValue
    }
  }

  /**
    * Update all columns of a dataframe.
    *  @param df: DataFrame to mutate.
    *  @param columnPathFilter: A function to filter what column paths are anonymized.
    *  @return: Mutated dataframe.
    */
  protected[this] def mutate(df: DataFrame, columnPathFilter: String => Boolean): DataFrame = {
    df.select(traverse(df.schema, columnPathFilter): _*)
  }

  // scalastyle:off cyclomatic.complexity
  /**
    * Traverse all columns of a dataframe schema and execute anonization functions based on column data types.
    *  @param schema: DataFrame schema.
    *  @param columnPathFilter: A function to filter what column paths are anonymized.
    *  @param path: Column path.
    *  @return: Anonymized columns.
    */
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
              case dt: StringType => AnonymizeStringUdf(c).as(f.name)
              case dt: ByteType => AnonymizeByteUdf(c).as(f.name)
              case dt: ShortType => AnonymizeShortUdf(c).as(f.name)
              case dt: IntegerType => AnonymizeIntegerUdf(c).as(f.name)
              case dt: LongType => AnonymizeLongUdf(c).as(f.name)
              case dt: FloatType => AnonymizeFloatUdf(c).as(f.name)
              case dt: DoubleType => AnonymizeDoubleUdf(c).as(f.name)
              case dt: DecimalType => AnonymizeDecimalUdf(c).as(f.name)
              case dt: DateType => AnonymizeDateUdf(c).as(f.name)
              case dt: TimestampType => AnonymizeTimestampUdf(c).as(f.name)
              case dt: MapType => anonymizeJson(c, f)
              case dt: ArrayType => anonymizeJson(c, f)
              case _ => c
            }
          } else {
            c
          }
      }
    })
  }
  // scalastyle:on cyclomatic.complexity

  protected[this] def anonymizeJson(c: Column, f: StructField): Column = {
    val json = to_json(c)
    val anonymizedJson = AnonymizeJsonStringUdf(json)
    from_json(anonymizedJson, f.dataType).as(f.name)
  }
}
