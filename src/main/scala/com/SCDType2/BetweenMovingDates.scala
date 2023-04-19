package com.SCDType2

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object BetweenMovingDates {

  def betweenMovingDates(historyDataFrame: DataFrame, updateDataFrame: DataFrame, spark: SparkSession): DataFrame = {

    val touchedHistory = historyDataFrame.select(col("Id"), col("firstName"), col("lastName"), col("address"),
      col("movedIn"), col("movedOut"))
    val touchedUpdate = updateDataFrame.select(col("newId").as("Id"), col("newFirstName").as("firstName"), col("newLastName").as("lastName"), col("newAddress").as("address"),
      col("newMovedIn").as("movedIn")).withColumn("movedOut", col("movedIn"))
    val unitedHistoryUpdates = touchedHistory.union(touchedUpdate)
    val windowSpec1 = Window.partitionBy("Id").orderBy(col("movedIn").desc)
    val movingInComparision = unitedHistoryUpdates.withColumn("nextAddress", lag("address", 1).over(windowSpec1))
      .withColumn("movingInComparision", lead("movedIn", 1).over(windowSpec1))
    val newRecordFirstPart = movingInComparision.select(col("Id"), col("firstName"), col("lastName"), col("address"),
      col("movedIn"), col("movingInComparision"), col("nextAddress")).where(to_date(col("movingInComparision"), "dd-MM-yyyy") > to_date(col("movedIn"), "dd-MM-yyyy") and
      (col("nextAddress") =!= col("address") or col("nextAddress").isNull))
      .withColumn("movedOut", col("movingInComparision"))
      .withColumn("status", lit(false))
      .drop(col("movingInComparision"))
      .drop(col("nextAddress"))
    val windowSpec2 = Window.partitionBy("Id").orderBy(col("movedOut").asc)
    val movingOutComparision = unitedHistoryUpdates.withColumn("nextAddress", lead("address", 1).over(windowSpec1))
      .withColumn("movingOutComparision", lag("movedOut", 1).over(windowSpec1))
    val newRecordSecondPart = movingOutComparision.select(col("Id"), col("firstName"), col("lastName"), col("nextAddress"),
      col("address"), col("movedIn"), col("movingOutComparision")).where(col("movingOutComparision") === "Null" or
      (to_date(col("movingOutComparision"), "dd-MM-yyyy") > to_date(col("movedIn"), "dd-MM-yyyy")) and
      (col("nextAddress") =!= col("address") or col("nextAddress").isNull))
      .withColumn("movedOut", col("movingOutComparision"))
      .withColumn("status", lit(true))
      .drop(col("nextAddress"))
      .drop(col("movingOutComparision"))
    val modifiedHistory = newRecordFirstPart.union(newRecordSecondPart)
    val bareHistory = historyDataFrame.join(modifiedHistory, historyDataFrame.col("Id") === modifiedHistory.col("Id"), "left_anti")
    val result = bareHistory.union(modifiedHistory)
    result
  }
}
