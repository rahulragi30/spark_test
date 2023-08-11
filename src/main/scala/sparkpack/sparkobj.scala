package sparkpack

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions._

object sparkobj {

  def main(args:Array[String]):Unit={


    println("=============Started1============")
    println

    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println

    val jsondf = spark
      .read
      .format("json")
      .option("multiline", "true")
      .load("C:/zeyo_data/cm.json")

    jsondf.show()
    jsondf.printSchema()


    val flatdf = jsondf.select(
      "Technology",
      "TrainerName",
      "address.*",
      "id",
      "workloc"

    )

    flatdf.show()
    flatdf.printSchema()

    val wcol = flatdf.withColumnRenamed("id", "id_no")
      .withColumn("permanent_chk", lit(col("permanent")))
        .withColumn("temporary_chk", lit("Chennai-temporary")).withColumn("workloc_chk", lit("pune-workloc"))


    wcol.show()
    wcol.printSchema()



  }

}
