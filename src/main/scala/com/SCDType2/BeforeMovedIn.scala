package com.SCDType2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object BeforeMovedIn {

  def beforeMovedIn(historyDataFrame: DataFrame, updateDataFrame: DataFrame, spark: SparkSession): DataFrame = {
    val joinedDataFrame = historyDataFrame.join(updateDataFrame, historyDataFrame.col("Id") === updateDataFrame.col("newId"), "outer")
    val addressChanged = when(col("address") =!= col("newAddress"), true).otherwise(false)
    val dfWithAddressChanged = joinedDataFrame.withColumn("addressChanged", addressChanged)
    val windowSpec = Window.partitionBy("Id").orderBy("movedIn")
    val dfWithPrevAddress = dfWithAddressChanged.withColumn("prevAddress", lag("address", 1).over(windowSpec))
    val result = dfWithPrevAddress.filter(col("addressChanged") === true)
      .select(col("Id"), col("newFirstName").as("firstName"), col("newLastName").as("lastName"),
        col("newAddress").as("address"), col("movedIn"), col("newMovedIn"))
      .withColumn("movedOut", col("movedIn"))
      .withColumn("movedIn", col("newMovedIn"))
      .withColumn("status", lit(false))
      .drop(col("newMovedIn"))
      .union(historyDataFrame)
    result
  }
}
