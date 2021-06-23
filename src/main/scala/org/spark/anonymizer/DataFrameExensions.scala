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

    def convertFirstName(
        columnPathFilter: String => Boolean = (p => true),
        serialRange: Integer = 0
    ): DataFrame = {
      new FirstNameConverter(OnlineNameDatabase, serialRange).convert(dataframe, columnPathFilter)
    }

    def convertLastName(
        columnPathFilter: String => Boolean = (p => true),
        serialRange: Integer = 100
    ): DataFrame = {
      new LastNameConverter(OnlineNameDatabase, serialRange).convert(dataframe, columnPathFilter)
    }

    def convertFullName(
        columnPathFilter: String => Boolean = (p => true),
        firstSerialRange: Integer = 0,
        lastSerialRange: Integer = 100
    ): DataFrame = {
      new FullNameConverter(OnlineNameDatabase, firstSerialRange, lastSerialRange)
        .convert(dataframe, columnPathFilter)
    }
  }
}