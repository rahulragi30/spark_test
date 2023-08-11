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

object ipl_joins {

  def main(args:Array[String]):Unit={



    println("=============Started1============")

    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")


    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val schema = StructType(
      Seq(
        StructField("Name", StringType, nullable = true),
        StructField("Age", IntegerType, nullable = true)
      )
    )


    val names = Seq("Alice", "Bob", "Charlie", "David")
    val ages = Seq(25, 30, 22, 28)


    val rows = names.zip(ages).map { case (name, age) => Row(name, age) }


    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)


    df.show()




    val ipldf = spark
      .read
      .option("header", true)
      .format("csv")
      .load("C:/zeyo_data/Match.csv")

    ipldf.show()
    ipldf.printSchema()

    val dbdf = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/demo")
      .option("dbtable", "student_prac")
      .option("user", "root")
      .option("password", "1234")
      //.option("batchsize", "10000")
      .option("fetchsize", "10000")
      .load()

    dbdf.show()


   """ipldf.write
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/demo")
      .option("dbtable", "ipl_data")
      .option("user", "root")
      .option("password", "1234")
      .save()

    """





  }
}
