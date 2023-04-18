package com.SCDType2

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object AfterMovedOut {

  def afterMovedOut(historyDataFrame: DataFrame, updateDataFrame: DataFrame, spark: SparkSession): DataFrame = {
    val newUpdates = updateDataFrame.select(col("newId").as("Id"),
      col("newFirstName").as("firstName"),
      col("newLastName").as("lastName"),
      col("newAddress").as("address"),
      col("newMovedIn").as("dateToCompareWith"))
    val newHistory = historyDataFrame.select(col("Id"),
      col("firstName"), col("lastName"), col("address"), col("movedOut").as("dateToCompareWith"))
    val UnitedHitsoryUpdates = newHistory.union(newUpdates)
    val windowSpec = Window.partitionBy("Id").orderBy("dateToCompareWith")
    val differentAddress = UnitedHitsoryUpdates.withColumn("nextAddress", lead("address", 1).over(windowSpec))
      .withColumn("nextMovingDate", lead("dateToCompareWith", 1).over(windowSpec))
    val newRecord = differentAddress.select(col("Id"), col("firstName"), col("lastName"), col("nextAddress")
      , col("nextMovingDate").as("movedIn"))
      .where(col("nextMovingDate").isNotNull)
      .where(col("nextAddress").isNotNull)
      .withColumn("address", col("nextAddress"))
      .withColumn("movedOut", lit("Null"))
      .withColumn("status", lit(true))
      .drop(col("nextAddress"))
    val modifiedUnitedHitsoryUpdates=historyDataFrame.union(newRecord)
    val windowSpec2 = Window.partitionBy("Id").orderBy("movedIn")
    val modifiedWindowSpec2 = modifiedUnitedHitsoryUpdates.withColumn("nextMovingDate", lead("movedIn", 1).over(windowSpec2))
    val modifiedHistory = modifiedWindowSpec2.select(col("Id"), col("firstName"), col("lastName"), col("movedIn"),
      col("movedOut"),col("address"))
      .where(col("nextMovingDate").isNotNull)
      .withColumn("status", lit(false))
    val bareHistory = historyDataFrame.join(modifiedHistory, historyDataFrame.col("Id") === modifiedHistory.col("Id"), "left_anti")
    val result = modifiedHistory.unionByName(newRecord).unionByName(bareHistory)
    result
  }
}
