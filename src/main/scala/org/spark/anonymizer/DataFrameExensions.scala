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
    
    def convertFirstName(columnPathFilter: String => Boolean): DataFrame = {
      new FirstNameConverter().convert(dataframe, columnPathFilter)
    }

    def convertLastName(columnPathFilter: String => Boolean): DataFrame = {
      new LastNameConverter().convert(dataframe, columnPathFilter)
    }

    def convertFullName(columnPathFilter: String => Boolean): DataFrame = {
      new FullNameConverter().convert(dataframe, columnPathFilter)
    }
  }
}