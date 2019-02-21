import org.apache.spark.sql.SparkSession

object Spark_DF {
  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()

  }
}
