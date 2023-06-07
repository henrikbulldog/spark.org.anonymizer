# Anonymizer

Anonymizes data in a DataFrame.

The method anonymize() will anonymize selected fields or all fields in a dataframe.    

## Typical use cases
Developers need production data to develop business logic.

Unit testing can get you a long way, but more often than not production data contains unforseen scenarios. The real world is messy and chaotic, and production data reflects that.

You may not be able to copy production data as-is due to legal and regulatory demands, hence the need for an anonymization utility.

## Usage
Get the jar from Maven by adding a reference to io.github.henrikbulldog:spark-org-anonymizer_2.12:1.0.2.

To anonymize all columns in a DataFrame, simply call the extension method DataFrame.anonymize.

```
import org.spark.anonymizer.DataFrame.Extensions

case class Info(id: Long, email: String, phone: String)
var df = Seq(Info(1234567890, "Firstname.Lastname@mail.com", "+45 1234 5678")).toDF
df.show(false)
val anonymed_df = df.anonymize()
anonymed_df.show(false)
```

Output:

```
+----------+---------------------------+
|id        |email                      |
+----------+---------------------------+
|1234567890|Firstname.Lastname@mail.com|
+----------+---------------------------+

+----------+---------------------------+
|id        |email                      |
+----------+---------------------------+
|1165749855|Etlkbwhcd.Qcfjczes@upcb.how|
+----------+---------------------------+
```

Notice that anonymization is format-preserving:
- Number of digits in the number is preserved
- Capital letters in the string are preserved
- Number of letters in the string are preserved
- Non-alphanumerics are preserved (. and @)

### Name Conversion
If you are not happy with random letters, but want real world names, you can do name conversions like this:
```
import org.spark.anonymizer.DataFrame.Extensions
import org.spark.anonymizer.StringNameDatabase
import scala.io.Source

val firstNames = Source.fromURL("https://raw.githubusercontent.com/henrikbulldog/spark.org.anonymizer/main/data/firstnames.us.txt").getLines.toSeq
val lastNames = Source.fromURL("https://raw.githubusercontent.com/henrikbulldog/spark.org.anonymizer/main/data/surnames.us.txt").getLines.toSeq
val nameDatabase = new StringNameDatabase(Some(firstNames), Some(lastNames))
var df = Seq((1, "Henrik", "Thomsen", "Henrik Thomsen"))
  .toDF("id", "firstname", "lastname", "fullname")
df.show(false)
val converted_df = df
  .convertFirstName(nameDatabase, p => p == "firstname")
  .convertLastName(nameDatabase, p => p == "lastname")
  .convertFullName(nameDatabase, p => p == "fullname")
converted_df.show(false)
```

Output:
```
+---+---------+--------+--------------+
|id |firstname|lastname|fullname      |
+---+---------+--------+--------------+
|1  |Henrik   |Thomsen |Henrik Thomsen|
+---+---------+--------+--------------+

+---+---------+--------+---------------+
|id |firstname|lastname|fullname       |
+---+---------+--------+---------------+
|1  |Kennith  |Hommell |Kennith Hommell|
+---+---------+--------+---------------+
```
### Anonymizing selected columns
To anonymize selected columns in a DataFrame, specify a filter method to DataFrame.anonymize:

```
import org.spark.anonymizer.Anonymizer.Extensions

case class Info(id: Long, email: String)
var df = Seq(Info(1234567890, "Firstname.Lastname@mail.com")).toDF
df.show(false)
df.anonymize((p => p != "id")).show(false)
```

Output:

```
+----------+---------------------------+
|id        |email                      |
+----------+---------------------------+
|1234567890|Firstname.Lastname@mail.com|
+----------+---------------------------+

+----------+---------------------------+
|id        |email                      |
+----------+---------------------------+
|1234567890|Etlkbwhcd.Qcfjczes@upcb.how|
+----------+---------------------------+
```

The filter (p => p != "id") anonymizes all columns except id.

## Implementation Stategies

### Irreversible
Anonymization is irreversible, anonymized data cannot be restored.

Algorithms that allow data to be restored (such as encryption) is sometimes referred to as pseudonymization.

### Format-preserving
Anonymization is format-preserving so that anonymized data has the look and feel of the original production data and can be used to reproduce prodcution scenarios.

The following formats are preserved:
- Lower case ASCII
- Upper case ASCII
- UTF letters
- Numbers
- Whitespaces, non-letters and non-numbers are not anonymized

For example email format is preserved: Donald.123.æøåÆØÅ@Duck.com -> Sijmhs.226.ÊºÚçÈÛ@Aair.szk

### Deterministic
Anonymization is deterministic; same input will allways yield same output. This is to support anonymization of keys and foreign keys.

## Supported data types
- Numeric types
  - ByteType: Represents 1-byte signed integer numbers. The range of numbers is from -128 to 127.
  - ShortType: Represents 2-byte signed integer numbers. The range of numbers is from -32768 to 32767. unscaled value and - a 32-bit integer scale.
  - IntegerType: Represents 4-byte signed integer numbers. The range of numbers is from -2147483648 to 2147483647.
  - LongType: Represents 8-byte signed integer numbers. The range of numbers is from -9223372036854775808 to 9223372036854775807.
  - FloatType: Represents 4-byte single-precision floating point numbers.
  - DoubleType: Represents 8-byte double-precision floating point numbers.
  - DecimalType: Represents arbitrary-precision signed decimal numbers. Backed internally by java.math.BigDecimal. A BigDecimal consists of an arbitrary precision integer
- String type
  - StringType: Represents character string values.
- Datetime type
  - TimestampType: Represents values comprising values of fields year, month, day, hour, minute, and second, with the session local time-zone. The timestamp value represents an absolute point in time.
  - DateType: Represents values comprising values of fields year, month and day, without a time-zone.

## Unsupported data types
- Binary type
  - BinaryType: Represents byte sequence values.
- Boolean type
  - BooleanType: Represents boolean values.
- Complex types
  - ArrayType(elementType, containsNull): Represents values comprising a sequence of elements with the type of elementType. containsNull is used to indicate if elements in a ArrayType value can have null values.
  - MapType(keyType, valueType, valueContainsNull): Represents values comprising a set of key-value pairs. The data type of keys is described by keyType and the data type of values is described by valueType. For a MapType value, keys are not allowed to have null values. valueContainsNull is used to indicate if values of a MapType value can have null values.
    
Maintainers
-------------------  
## Release Preparation
- Sonatype credentials need to be defined inside global sbt settings While publishing the artifact with sbt, sbt-sonatype can pick them up from the $HOME/.sbt/(sbt-version 0.13 or 1.0)/sonatype.sbt file Like below .
          `credentials += Credentials(
              "Sonatype Nexus Repository Manager",
               "sonatype repository url",
              "<sonatype username>",
              "<sonatype password>"
  )`
- Configure PGP key pair to sign the artifacts using sbt-pgp plugin.

## Releases
Maintainers must run `sbt release` to release the artifacts to maven central.It will asks current version, next version of the artifacts.
It's to be noted that `sbt release` will only release artifcats with recent scala version mentioned in `.travis.yml`.To release in multiple versions of scala we should use `sbt +release`

