package com.SCDType2

import com.SCDType2.DifferentId.differentId
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DifferentIdSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("AddressHistoryBuildeTest")
    .getOrCreate()

  import spark.implicits._

  "differentId" should "2L, \"Comblet\", \"Fabrice\", \"France\", \"06-07-2012\", \"Null\", true" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", "21-06-2014", true)).toDF()
    val updatesDataframe = Seq(Updates(2L, "Comblet", "Fabrice", "France", "06-07-2012")).toDF()
    When("differentId is invoked")
    val result = differentId(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", "21-06-2014", true),
      History(2L, "Comblet", "Fabrice", "France", "06-07-2012", "Null", true)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}
