package com.SCDType2

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Duplicates {

  def duplicates(updatesDataframe: DataFrame, spark: SparkSession): DataFrame = {

    val windowSpec = Window.partitionBy("newId").orderBy("newMovedIn")
    val orderedUpdates = updatesDataframe.withColumn("prevdates", lag("newMovedIn", 1).over(windowSpec))
    val unduplicateUpdates=orderedUpdates.where(col("prevdates").isNull)
        .drop(col("prevdates"))
    unduplicateUpdates
  }
}
