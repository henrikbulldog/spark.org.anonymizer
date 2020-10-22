package org.spark

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.sql.{Date, Timestamp}
import java.math.BigDecimal

/**
  * Anonymizes selected columns in a dataframe while preserving format.
  *
  * To anonymize selected columns in a dataframe:
  * val df = Anonymizer.anonymize(input_df, (p => Array("col1", "col2").contains(p)))
  *
  * To anonymize all columns in a dataframe:
  * val df = Anonymizer.anonymize(input_df)
  *
  * To anonymize all columns in a dataframe except one:
  * val df = Anonymizer.anonymize(input_df, (p => p != "id"))
  *
  * To anonymize a single column:
  * df.withColumn("anonymized_col1", Anonymizer.anonymize_string_udf($"col1"))
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
  val anonymize_string_udf = udf(anonymize_string _)

  /**
    * UDF to anonymize a Long while preserving its number of digits.
    *  @param s: Column of type Long to anonymize.
    *  @return: Anonymized Long.
    */
  val anonymize_long_udf = udf(anonymize_long _)

  /**
    * UDF to anonymize an Integer while preserving its number of digits.
    *  @param s: Column of type Integer to anonymize.
    *  @return: Anonymized Integer.
    */  
  val anonymize_integer_udf = udf[Integer, Integer](i => anonymize_long(i.toLong).toInt)

  /**
    * UDF to anonymize an Short while preserving its number of digits.
    *  @param s: Column of type Short to anonymize.
    *  @return: Anonymized Short.
    */  
  val anonymize_short_udf = udf[Short, Short](i => anonymize_long(i.toLong).toShort)

  /**
    * UDF to anonymize an Byte while preserving its number of digits.
    *  @param s: Column of type Byte to anonymize.
    *  @return: Anonymized Byte.
    */  
  val anonymize_byte_udf = udf[Byte, Byte](i => anonymize_long(i.toLong).toByte)

  /**
    * UDF to anonymize a Float.
    *  @param s: Column of type Float to anonymize.
    *  @return: Anonymized Float.
    */  
  val anonymize_float_udf = udf[Float, Float](d => anonymize_double(d).toFloat)

  /**
    * UDF to anonymize a Double.
    *  @param s: Column of type Double to anonymize.
    *  @return: Anonymized Double.
    */  
  val anonymize_double_udf = udf(anonymize_double _)

  /**
    * UDF to anonymize a Decimal.
    *  @param s: Column of type Decimal to anonymize.
    *  @return: Anonymized Decimal.
    */  
  val anonymize_decimal_udf = udf[BigDecimal, BigDecimal](d => new BigDecimal(anonymize_double(d.doubleValue)))

  /**
    * UDF to anonymize a Timestamp.
    *  @param s: Column of type Timestamp to anonymize.
    *  @return: Anonymized Timestamp.
    */  
  val anonymize_timestamp_udf = udf(anonymize_timestamp _)

  /**
    * UDF to anonymize a Date.
    *  @param s: Column of type Date to anonymize.
    *  @return: Anonymized Date.
    */  
  val anonymize_date_udf = udf[Date, Date](dt => new Date(anonymize_timestamp(new Timestamp(dt.getTime)).get.getTime))

  private[this] val ascii_upper_letters = ('A' to 'Z').toList.filter(_.isLetter)
  private[this] val ascii_lower_letters = ('a' to 'z').toList.filter(_.isLetter)
  private[this] val utf_letters = (128.toChar to 256.toChar).toList.filter(_.isLetter)
  private[this] val numbers = ('0' to '9')

  /**
    * Anonymize a string while preserving its format.
    *  @param s: String to anonymize.
    *  @return: Anonymized string.
    */
  def anonymize_string(s: String): Option[String] = {
    s match {
      case null => None
      case _ => {
        val seed = scala.util.hashing.MurmurHash3.stringHash(s).abs
        val random = new scala.util.Random(seed)
        var r = ""
        for (c <- s) {
          if (numbers.contains(c)) {
            r = r + (((random.nextInt.abs + c) % numbers.size))
          } else if (ascii_upper_letters.contains(c)) {
            r = r + ascii_upper_letters(((random.nextInt.abs) % ascii_upper_letters.size))
          } else if (ascii_lower_letters.contains(c)) {
            r = r + ascii_lower_letters(((random.nextInt.abs) % ascii_lower_letters.size))
          } else if (utf_letters.contains(c)) {
            r = r + utf_letters(((random.nextInt.abs) % utf_letters.size))
          } else {
            r = r + c
          }
        }
        Some(r)
      }
    }
  }

  def anonymize_long(l: Long): Long = {
    val seed = scala.util.hashing.MurmurHash3.stringHash(l.toString).abs
    val random = new scala.util.Random(seed)
    random.nextLong.abs % l * (l/l.abs)
  }

  def anonymize_double(d: Double): Double = {
    val seed = scala.util.hashing.MurmurHash3.stringHash(d.toString).abs
    val random = new scala.util.Random(seed)
    random.nextDouble.abs % d * (d/d.abs)
  }

  def anonymize_timestamp(ts: Timestamp): Option[Timestamp] = {
    ts match {
      case null => None
      case _ => {
        val seed = scala.util.hashing.MurmurHash3.stringHash(ts.toString).abs
        val random = new scala.util.Random(seed)
        val ms = ts.getTime()
        Some(new Timestamp((random.nextLong % (ms/2)) + (ms/2)))
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
    df.sqlContext.createDataFrame(df.select(traverse(df.schema, columnPathFilter): _*).rdd, df.schema)
  }

  /**
    * Traverse all columns of a dataframe schema and execute anonization functions based on column data types.
    *  @param schema: DataFrame schema.
    *  @param columnPathFilter: A function to filter what column paths are anonymized.
    *  @param path: Column path.
    *  @return: Anonymized columns.
    */
  private[this] def traverse(schema: StructType, columnPathFilter: String => Boolean, path: String = ""): Array[Column] = {
    schema.fields.map(f => {
      val c = col(path + f.name)
        f.dataType match {
          case s: StructType => when(c.isNotNull, struct(traverse(s, columnPathFilter, path + f.name + "."): _*))
          case _ => if(columnPathFilter(path + f.name)) {
              f.dataType match {
              case s: StringType => anonymize_string_udf(c)
              case s: ByteType => anonymize_byte_udf(c)
              case s: ShortType => anonymize_short_udf(c)
              case s: IntegerType => anonymize_integer_udf(c)
              case s: LongType => anonymize_long_udf(c)
              case s: FloatType => anonymize_float_udf(c)
              case s: DoubleType => anonymize_double_udf(c)
              case s: DecimalType => anonymize_decimal_udf(c)
              case s: DateType => anonymize_date_udf(c)
              case s: TimestampType => anonymize_timestamp_udf(c)
              case _ => c
            }
          } else {
            c
          }
        }
    })
  }
}