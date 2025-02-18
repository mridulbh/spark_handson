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

    cust_df.show(10)

    cust_df.filter("Company is not null").show(10,truncate = false)

    val invoice_df = spark.read.format("jdbc")
      .option("mode","permissive")
      .option("url",jdbcUrl)
      .option("dbtable","Invoice")
      .option("driver","org.sqlite.JDBC")
      .load()

    invoice_df.show(5)

    ///Joining Customer and Invoice table from DB
    cust_df.join(invoice_df,cust_df("CustomerId") === invoice_df("CustomerId")).show(5)


    scala.io.StdIn.readLine("Enter to terminate the session..")

  }
}