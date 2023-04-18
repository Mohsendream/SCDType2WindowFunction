package com.SCDType2

import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.SCDType2.AfterMovedOut.afterMovedOut


class AfterMovedOutSpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Test1")
    .getOrCreate()
  import spark.implicits._
  "AfterMovedOutSpec" should "return  1 Madiouni Mohsen Tunis 16-09-2014 Null true" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", "16-09-2014", true),
      History(2L, "Boukari", "Dorra", "France", "16-04-2023", "Null", true)).toDF()
    val updatesDataframe = Seq(Updates(1L, "Madiouni", "Mohsen", "Tunis", "16-09-2014")).toDF()
    val joineddataframe= historyDataframe.join(updatesDataframe, historyDataframe.col("Id") === updatesDataframe.col("newId"), "outer")
    When("BeforeMovedInSpec is invoked")
    val result = afterMovedOut(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", "16-09-2014", false),
      History(2L, "Boukari", "Dorra", "France", "16-04-2023", "Null", true),
      History(1L, "Madiouni", "Mohsen", "bouarada", "16-09-2014", "Null", true)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}
