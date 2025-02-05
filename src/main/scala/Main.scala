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

    import spark.implicits._

    val flight_schema = StructType(Array(
      StructField("DEST_CNTRY_NM",StringType,nullable = true),
      StructField("ORIG_CNTRY_NM",StringType,nullable = true),
      StructField("COUNT",IntegerType,nullable = true),
    ))

    val flight_df = spark.read.format("csv")
      .option("mode","permissive")
      .option("infer schema",value = false)
      .option("header",value= true)
      .schema(flight_schema)
      .load("data/2010-summary.csv")
    flight_df.show(5)

    val filter_df = flight_df.filter($"COUNT">1)
    filter_df.show(2)


    val emp_schema = StructType(Array(
      StructField("Id",IntegerType,nullable=true),
      StructField("Name",StringType,nullable=true),
      StructField("Age",IntegerType,nullable=true),
      StructField("Salary",IntegerType,nullable=true),
      StructField("Address",StringType,nullable=true),
      StructField("Nominee",StringType,nullable=true),
      StructField("_corrupt_record",StringType,nullable=true)
    ))

    val emp_df = spark.read.format("csv")
      .option("mode","permissive")
      .option("header",value = true)
      .option("inferSchema", value = false)
      .schema(emp_schema)
      .load("data/emp_data.csv")
    emp_df.show(false)

    val corrupt_rec_df = emp_df.filter("_corrupt_record is not null")
    corrupt_rec_df.show()

    corrupt_rec_df.write.format("csv")
      .option("header",value= true)
      .mode("overwrite")
      .save("data/corrupt_rec")

    val clean_df = emp_df.filter("_corrupt_record is null").drop("_corrupt_record")
    clean_df.show()

    clean_df.write.format("csv")
      .option("header",value = true)
      .mode("overwrite")
      .save("data/clean_emp")



    scala.io.StdIn.readLine("Press enter to terminate the Spark session...")

  }
}