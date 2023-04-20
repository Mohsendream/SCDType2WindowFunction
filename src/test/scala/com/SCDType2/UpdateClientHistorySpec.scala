package com.SCDType2

import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.SCDType2.UpdateClientHistory.updateClientHistory

case class History(Id: Long, firstName: String, lastName: String, address: String, movedIn: String, movedOut: String, status: Boolean)

case class Updates(Id: Long, firstName: String, lastName: String, address: String, movedIn: String)

class UpdateClientHistorySpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Test1")
    .getOrCreate()

  import spark.implicits._

  "Test1" should "insert new records with different Ids" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", null, true)).toDF()
    val updatesDataframe = Seq(Updates(2L, "Comblet", "Fabrice", "France", "06-07-2012")).toDF()
    When("differentId is invoked")
    val result = updateClientHistory(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", null, true),
      History(2L, "Comblet", "Fabrice", "France", "06-07-2012", null, true)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }

  "Test2" should "return the after movedOut correct" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", null, true)).toDF()
    val updatesDataframe = Seq(Updates(1L, "Madiouni", "Mohsen", "France", "21-06-2014")).toDF()
    When("differentId is invoked")
    val result = updateClientHistory(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", "21-06-2014", false),
      History(1L, "Madiouni", "Mohsen", "France", "21-06-2014", null, true)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
  "Test3" should "return  the between dates first test" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", null, true)).toDF()
    val updatesDataframe = Seq(Updates(1L, "Madiouni", "Mohsen", "Tunis", "06-07-2012")).toDF()
    When("differentId is invoked")
    val result = updateClientHistory(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "Tunis", "06-07-2012", null, true),
      History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", "06-07-2012", false)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }



  "Test4" should "return  before moved in date" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", null, true)).toDF()
    val updatesDataframe = Seq(Updates(1L, "Madiouni", "Mohsen", "bouarada", "06-07-1995")).toDF()
    When("differentId is invoked")
    val result = updateClientHistory(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", null, true),
      History(1L, "Madiouni", "Mohsen", "bouarada", "06-07-1995", "15-09-2010", false)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
  "Test6" should "return a history table updates with the first date when the same address" in {
    Given("the history and the updates dataframes")
    val updatesDataframe = Seq(Updates(1L, "Madiouni", "Mohsen", "Kef", "15-09-2009")).toDF()
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", null, true),
      History(2L, "Madiouni", "Haifa", "Liege", "15-09-2020", "Null", true)).toDF()
    When("duplicates is invoked")
    val result = updateClientHistory(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2009", null, true),
      History(2L, "Madiouni", "Haifa", "Liege", "15-09-2020", "Null", true)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }

  "Test7" should "all cases" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(
      History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", null, true),
      History(2L, "Madiouni", "Haifa", "Liege", "10-10-2021", null, true),
      History(3L, "Boukari", "Dorra", "France", "16-14-2023", null, true)
    ).toDF()
    val updatesDataframe = Seq(
      Updates(1L, "Madiouni", "Mohsen", "Tunis", "15-09-2005"),
      Updates(2L, "Madiouni", "Haifa", "Liege", "15-09-2005")
    ).toDF()

    When("differentId is invoked")
    val result = updateClientHistory(historyDataframe, updatesDataframe, spark)

    Then("the result should be returned")
    val expectedResult = Seq(
      History(1L, "Madiouni", "Mohsen", "Tunis", "15-09-2005", "15-09-2010", false),
      History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", null, true),
      History(2L, "Madiouni", "Haifa", "Liege", "15-09-2005", null, true),
      History(3L, "Boukari", "Dorra", "France", "16-14-2023", null, true)
    ).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }

}