package sparkpack

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions._

object nyc_data {

  def main(args:Array[String]):Unit={


    println("=============Started1============")
    println

    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println

    val pardf = spark
      .read
      .format("parquet")
      .load("C:/Users/Rahul/Downloads/nyc_data.parquet")

    pardf.show()
    pardf.printSchema()

    val cdf = pardf.withColumn("pickup_date", date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd"))


    cdf.na.fill("").write.mode("overwrite").partitionBy("pickup_date").parquet(f"C:/Users/Rahul/Downloads/demo_data_nyc")
    cdf.printSchema()



    // cdf.write.mode("overwrite").partitionBy("pickup_date").csv(f"C:/Users/Rahul/Downloads/demo_data_nyc")

    //spark.stop()


  }
}
