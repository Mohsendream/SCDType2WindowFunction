package com.SCDType2

import com.SCDType2.BetweenMovingDates.betweenMovingDates
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BewteenMovingDatesSpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("AddressHistoryBuildeTest")
    .getOrCreate()

  import spark.implicits._

  "BewteenMovingDates Test1" should "return  1 Madiouni Mnchohsen kef 15-09-2010 06-07-2012  false" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", "21-06-2014", true)).toDF()
    val updatesDataframe = Seq(Updates(1L, "Madiouni", "Mohsen", "Tunis", "06-07-2012")).toDF()
    When("BewteenMovingDates is invoked")
    val result = betweenMovingDates(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "Tunis", "06-07-2012", "21-06-2014", true),
      History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", "06-07-2012", false)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
  "BewteenMovingDates test 2" should "return  1 Madiouni Mohsen kef 15-09-2010 06-07-2012  false" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", "Null", true)).toDF()
    val updatesDataframe = Seq(Updates(1L, "Madiouni", "Mohsen", "Tunis", "06-07-2012")).toDF()
    val joinedDataFrame = historyDataframe.join(updatesDataframe, historyDataframe.col("Id") === updatesDataframe.col("newId"), "inner")
    When("BewteenMovingDates is invoked")
    val result = betweenMovingDates(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "Tunis", "06-07-2012", "Null", true),
      History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", "06-07-2012", false)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}

