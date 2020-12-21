package com.laerdal.spark.utils

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.sql.{Date, Timestamp}
import java.math.BigDecimal
import scala.math.signum

/**
  * Anonymizes selected columns in a dataframe while preserving format.
  *
  * To anonymize selected columns in a dataframe:
  *
  * import henrik.thomsen.spark.utils.Anonymizer.Extensions
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
  * import henrik.thomsen.spark.utils.Anonymizer.Extensions
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
    *  @param s: Column of type String to anonymize.
    *  @return: Anonymized string.
    */
  val AnonymizeStringUdf = udf(anonymizeString _)

  /**
    * UDF to anonymize a Long while preserving its number of digits.
    *  @param s: Column of type Long to anonymize.
    *  @return: Anonymized Long.
    */
  val AnonymizeLongUdf = udf(anonymizeLong _)

  /**
    * UDF to anonymize an Integer while preserving its number of digits.
    *  @param s: Column of type Integer to anonymize.
    *  @return: Anonymized Integer.
    */
  val AnonymizeIntegerUdf = udf[Integer, Integer](i => anonymizeLong(i.toLong).toInt)

  /**
    * UDF to anonymize an Short while preserving its number of digits.
    *  @param s: Column of type Short to anonymize.
    *  @return: Anonymized Short.
    */
  val AnonymizeShortUdf = udf[Short, Short](i => anonymizeLong(i.toLong).toShort)

  /**
    * UDF to anonymize an Byte while preserving its number of digits.
    *  @param s: Column of type Byte to anonymize.
    *  @return: Anonymized Byte.
    */
  val AnonymizeByteUdf = udf[Byte, Byte](i => anonymizeLong(i.toLong).toByte)

  /**
    * UDF to anonymize a Float.
    *  @param s: Column of type Float to anonymize.
    *  @return: Anonymized Float.
    */
  val AnonymizeFloatUdf = udf[Float, Float](d => anonymizeDouble(d).toFloat)

  /**
    * UDF to anonymize a Double.
    *  @param s: Column of type Double to anonymize.
    *  @return: Anonymized Double.
    */
  val AnonymizeDoubleUdf = udf(anonymizeDouble _)

  /**
    * UDF to anonymize a Decimal.
    *  @param s: Column of type Decimal to anonymize.
    *  @return: Anonymized Decimal.
    */
  val AnonymizeDecimalUdf =
    udf[BigDecimal, BigDecimal](d => new BigDecimal(anonymizeDouble(d.doubleValue)))

  /**
    * UDF to anonymize a Timestamp.
    *  @param s: Column of type Timestamp to anonymize.
    *  @return: Anonymized Timestamp.
    */
  val AnonymizeTimestampUdf = udf(anonymizeTimestamp _)

  /**
    * UDF to anonymize a Date.
    *  @param s: Column of type Date to anonymize.
    *  @return: Anonymized Date.
    */
  val AnonymizeDateUdf = udf[Option[Date], Date](dt => {
    if (dt == null) {
      None
    } else {
      Some(new Date(anonymizeTimestamp(new Timestamp(dt.getTime)).get.getTime))
    }
  })

  private[this] val AsciiUpperLetters = ('A' to 'Z').toList.filter(_.isLetter)
  private[this] val AsciiLowerLetters = ('a' to 'z').toList.filter(_.isLetter)
  private[this] val UtfLetters = (128.toChar to 256.toChar).toList.filter(_.isLetter)
  private[this] val Numbers = ('0' to '9')

  /**
    * Anonymize a string while preserving its format.
    *  @param s: String to anonymize.
    *  @return: Anonymized string.
    */
  def anonymizeString(s: String): Option[String] = {
    s match {
      case null => None
      case _ => {
        val seed = scala.util.hashing.MurmurHash3.stringHash(s).abs
        val random = new scala.util.Random(seed)
        var r = ""
        for (c <- s) {
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
  }

  val MinNumber = 128

  def anonymizeLong(l: Long): Long = {
    if (l == 0) {
      0
    }
    val seed = scala.util.hashing.MurmurHash3.stringHash(l.toString).abs
    val random = new scala.util.Random(seed)
    random.nextLong.abs % math.max(l.abs, MinNumber) * signum(l)
  }

  def anonymizeDouble(d: Double): Double = {
    if (d == 0) {
      0
    }
    val seed = scala.util.hashing.MurmurHash3.stringHash(d.toString).abs
    val random = new scala.util.Random(seed)
    random.nextDouble.abs % math.max(d.abs, MinNumber) * signum(d)
  }

  def anonymizeTimestamp(ts: Timestamp): Option[Timestamp] = {
    ts match {
      case null => None
      case _ => {
        val seed = scala.util.hashing.MurmurHash3.stringHash(ts.toString).abs
        val random = new scala.util.Random(seed)
        val ms = ts.getTime()
        Some(new Timestamp((random.nextLong % (ms / 2)) + (ms / 2)))
      }
    }
  }

  /**
    * Update all columns of a dataframe.
    *  @param df: DataFrame to mutate.
    *  @param columnPathFilter: A function to filter what column paths are anonymized.
    *  @return: Mutated dataframe.
    */
  private[this] def mutate(df: DataFrame, columnPathFilter: String => Boolean): DataFrame = {
    df.sqlContext
      .createDataFrame(df.select(traverse(df.schema, columnPathFilter): _*).rdd, df.schema)
  }

  /**
    * Traverse all columns of a dataframe schema and execute anonization functions based on column data types.
    *  @param schema: DataFrame schema.
    *  @param columnPathFilter: A function to filter what column paths are anonymized.
    *  @param path: Column path.
    *  @return: Anonymized columns.
    */
  private[this] def traverse(
      schema: StructType,
      columnPathFilter: String => Boolean,
      path: String = ""
  ): Array[Column] = {
    schema.fields.map(f => {
      val c = col(path + f.name)
      f.dataType match {
        case s: StructType =>
          when(c.isNotNull, struct(traverse(s, columnPathFilter, path + f.name + "."): _*))
        case _ =>
          if (columnPathFilter(path + f.name)) {
            f.dataType match {
              case s: StringType => AnonymizeStringUdf(c)
              case s: ByteType => AnonymizeByteUdf(c)
              case s: ShortType => AnonymizeShortUdf(c)
              case s: IntegerType => AnonymizeIntegerUdf(c)
              case s: LongType => AnonymizeLongUdf(c)
              case s: FloatType => AnonymizeFloatUdf(c)
              case s: DoubleType => AnonymizeDoubleUdf(c)
              case s: DecimalType => AnonymizeDecimalUdf(c)
              case s: DateType => AnonymizeDateUdf(c)
              case s: TimestampType => AnonymizeTimestampUdf(c)
              case _ => c
            }
          } else {
            c
          }
      }
    })
  }

  implicit class Extensions(dataframe: DataFrame) {

    /**
      * Anonymize selected fields in a dataframe.
      *  @param columnPathFilter: A function to filter what column paths are anonymized.
      *  @return: Anonymized dataframe.
      */
    def anonymize(columnPathFilter: String => Boolean = (p => true)): DataFrame = {
      Anonymizer.anonymize(dataframe, columnPathFilter)
    }
  }
}
