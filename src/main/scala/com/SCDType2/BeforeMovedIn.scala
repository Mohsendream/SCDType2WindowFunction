package com.SCDType2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object BeforeMovedIn {

  def beforeMovedIn(historyDataFrame: DataFrame, updateDataFrame: DataFrame, spark: SparkSession): DataFrame = {
    val newHistory = historyDataFrame.drop(col("movedOut"))
      .drop(col("Status"))
    val newUpdates = updateDataFrame.select(col("newId").as("Id"),
      col("newFirstName").as("firstName"),
      col("newLastName").as("lastName"),
      col("newAddress").as("address"),
      col("newMovedIn").as("movedIn"))
    val UnitedHitsoryUpdates = newHistory.union(updateDataFrame)
    val windowSpec = Window.partitionBy("Id").orderBy("movedIn")
    val differentAddress = UnitedHitsoryUpdates.withColumn("nextAddress", lead("address", 1).over(windowSpec))
      .withColumn("nextMovingDate", lead("movedIn", 1).over(windowSpec))
    val newRecord = differentAddress.select(col("Id"), col("firstName"), col("lastName"), col("address"),
      col("movedIn"),  col("nextMovingDate"))
      .where(col("nextAddress").isNotNull)
      .withColumn("movedOut", col("nextMovingDate"))
      .withColumn("status", lit(false))
      .drop( col("nextMovingDate"))
    val result = historyDataFrame.union(newRecord)
    result
  }
}
