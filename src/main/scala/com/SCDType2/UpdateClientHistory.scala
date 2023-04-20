package com.SCDType2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object UpdateClientHistory {

  def updateClientHistory(historyDataFrame: DataFrame, updatedDataframe: DataFrame, spark: SparkSession): DataFrame = {

    val cleanedUpdated= updatedDataframe.dropDuplicates()

    val joinedHistoryUpdates = historyDataFrame.join(cleanedUpdated, historyDataFrame.col("Id") === cleanedUpdated.col("Id"), "inner")
      .dropDuplicates()

    val touchedHistory = joinedHistoryUpdates
      .drop(cleanedUpdated.col("Id"))
      .drop(cleanedUpdated.col("firstName"))
      .drop(cleanedUpdated.col("lastName"))
      .drop(cleanedUpdated.col("address"))
      .drop(cleanedUpdated.col("movedIn"))
      .drop(historyDataFrame.col("status"))
      .drop("movedOut")
      .unionByName(cleanedUpdated)
      .dropDuplicates()

    val lateArrivingSameIdAddressDataframe = joinedHistoryUpdates
      .where(cleanedUpdated.col("address") === historyDataFrame.col("address"))
      .where(to_date(cleanedUpdated.col("movedIn"), "dd-MM-yyyy") <= to_date(historyDataFrame.col("movedIn"), "dd-MM-yyyy"))

    val sameAddressUpdates = lateArrivingSameIdAddressDataframe.select(historyDataFrame.col("Id"),
      cleanedUpdated.col("firstName"), cleanedUpdated.col("lastName"),
      cleanedUpdated.col("address"), cleanedUpdated.col("movedIn"))


    val historyWithoutSameaddress=touchedHistory.join(sameAddressUpdates, touchedHistory.col("Id") === sameAddressUpdates.col("Id"), "left_anti")

    val workingHistory=historyWithoutSameaddress.unionByName(sameAddressUpdates)

    val windowSpec = Window.partitionBy("Id").orderBy( to_date(col("movedIn"), "dd-MM-yyyy").asc)

    val modifiedHistory = workingHistory.withColumn("movedOut", lead("movedIn", 1).over(windowSpec))
      .withColumn("nextAddress", lead("address", 1).over(windowSpec))
      .withColumn("status", when((col("movedOut").isNotNull or col("movedOut") >= col("movedIn"))
        and (col("nextAddress") =!= col("address") and col("nextAddress").isNotNull), false).otherwise(true))
        .drop(col("nextAddress"))

    val bareHistory = historyDataFrame.join(modifiedHistory, historyDataFrame.col("Id") === modifiedHistory.col("Id"), "left_anti")

    val result = modifiedHistory.unionByName(bareHistory)

    result
  }
}
