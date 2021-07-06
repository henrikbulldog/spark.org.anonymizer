package org.spark.anonymizer

import org.apache.spark.sql.DataFrame

object DataFrame {
  implicit class Extensions(dataframe: DataFrame) {
    /**
      * Anonymize selected fields in a dataframe.
      *  @param columnPathFilter: A function to filter what column paths are anonymized.
      *  @return: Anonymized dataframe.
      */
    def anonymize(columnPathFilter: String => Boolean = (p => true)): DataFrame = {
      Anonymizer.anonymize(dataframe, columnPathFilter)
    }

    def convertName(
        columnPathFilter: String => Boolean = (p => true),
        names: Seq[String],
        serialRange: Option[Integer] = None
    ): DataFrame = {
      new NameConverter(Seq(NameList(names, serialRange))).convert(dataframe, columnPathFilter)
    }

    def convertFullName(
        columnPathFilter: String => Boolean = (p => true),
        firstNames: Seq[String],
        firstSerialRange: Option[Integer] = None,
        lastNames: Seq[String],
        lastSerialRange: Option[Integer] = None
    ): DataFrame = {
      new NameConverter(
        Seq(NameList(firstNames, firstSerialRange), NameList(lastNames, lastSerialRange)))
        .convert(dataframe, columnPathFilter)
    }
  }
}
