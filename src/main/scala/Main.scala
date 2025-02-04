package de.spark_handson

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Main {
  def main(args: Array[String]): Unit={

    val spark = SparkSession
      .builder()
      .appName("Spark-App")
      .master("local[*]")
      .getOrCreate()

//    import org.apache.spark.implicits._

    val my_schema = StructType(Array(
      StructField("DEST_CNTRY_NM",StringType,nullable = true),
      StructField("ORIG_CNTRY_NM",StringType,nullable = true),
      StructField("COUNT",IntegerType,nullable = true)
    ))

    val df = spark.read.format("csv")
      .option("mode","permissive")
      .option("infer schema",value = false)
      .option("header",value= true)
      .schema(my_schema)
      .load("data/2010-summary.csv")



    df.show(5)
  }
}