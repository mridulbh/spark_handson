package de.spark_handson

import org.apache.spark.sql.SparkSession

object sqlDb {

  def main (args:Array[String]): Unit ={

    val spark = SparkSession
      .builder()
      .appName("sqlDB")
      .master("local[*]")
      .getOrCreate()

    val db_path = "/Users/mridulbh/Library/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db"

    val jdbcUrl = s"jdbc:sqlite:$db_path"

    val cust_df = spark.read.format("jdbc")
      .option("url",jdbcUrl)
      .option("driver","org.sqlite.JDBC")
      .option("mode","permissive")
      .option("dbtable","Customer")
      .load()

    cust_df.show(5)


  }
}