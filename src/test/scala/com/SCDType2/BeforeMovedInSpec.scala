package com.SCDType2

import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.SCDType2.BeforeMovedIn.beforeMovedIn

case class History(Id: Long, firstName: String, lastName: String, address: String, movedIn: String, movedOut: String, status: Boolean)

case class Updates(newId: Long, newFirstName: String, newLastName: String, newAddress: String, newMovedIn: String)

class BeforeMovedInSpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Test1")
    .getOrCreate()
  import spark.implicits._
  "BeforeMovedInSpec" should "return  1 Madiouni Mohsen Bouarada 06-07-1995 15-09-2010 false" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", "Null", true),
      History(2L, "Boukari", "Dorra", "France", "16-04-2023", "Null", true)).toDF()
    val updatesDataframe = Seq(Updates(1L, "Madiouni", "Mohsen", "bouarada", "06-07-1995")).toDF()
    When("BeforeMovedInSpec is invoked")
    val result = beforeMovedIn(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", "Null", true),
      History(2L, "Boukari", "Dorra", "France", "16-04-2023", "Null", true),
      History(1L, "Madiouni", "Mohsen", "bouarada", "06-07-1995", "15-09-2010", false)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}