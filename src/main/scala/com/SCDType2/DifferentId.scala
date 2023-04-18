package com.SCDType2

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DifferentId {

  def differentId(historyDataframe: DataFrame, unduplicateUpdates: DataFrame, spark: SparkSession): DataFrame = {

    val newUpdatesDifferentId = unduplicateUpdates.join(historyDataframe, col("id") === col("newId"), "left_anti")
    val newRecordsUpdated = newUpdatesDifferentId.select("*")
      .withColumn("movedOut", lit("Null"))
      .withColumn("status", lit(true))
    val newHistory = historyDataframe.union(newRecordsUpdated)
    newHistory
  }
}
