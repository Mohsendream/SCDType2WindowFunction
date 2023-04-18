package com.SCDType2

import com.SCDType2.Duplicates.duplicates
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DuplicatesSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("DuplicatesTest")
    .getOrCreate()

  import spark.implicits._

  "DuplicatesSpec" should "delete the duplicate records" in {
    Given("the history dataframe and the updates dataframe")
    val updatesDataframe = Seq(Updates(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010"),
      Updates(1L, "Madiouni", "Mohsen", "Kef", "15-09-2009"),
      Updates(2L, "Mike", "Tyson", "LA", "15-09-2010")).toDF()
    When("duplicates is invoked")
    val result = duplicates(updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(Updates(1L, "Madiouni", "Mohsen", "Kef", "15-09-2009"),
      Updates(2L, "Mike", "Tyson", "LA", "15-09-2010")).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}
