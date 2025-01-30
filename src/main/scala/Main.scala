package de.spark_handson

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit ={

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("sparkHandon")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header",value = true)
      .load("/Users/mridulbh/Data_Engg/spark_handson/data")

    df.show(5)


  }
}
