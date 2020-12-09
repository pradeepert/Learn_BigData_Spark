import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.IntegerType

object DataFrameSG {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

//This below lines will help you to copy paste for CLI
val cus = spark.read.format("csv").option("sep", ";").option("inferSchema", "true").option("header", "true").load("customer")
val acc = spark.read.format("csv").option("sep", ";").option("inferSchema", "true").option("header", "true").load("Account")
val prodRule = spark.read.format("csv").option("sep", ";").option("inferSchema", "true").option("header", "true").load("Product Rules")
val stat = spark.read.format("csv").option("sep", ";").option("inferSchema", "true").option("header", "true").load("Statement")

    // Customer
    val cus = spark.read
      .format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("customer")


    // Account
    val acc = spark
      .read
      .format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("Account")

    // Product Rules
    val prodRule = spark
      .read
      .format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("Product Rules")
    // Product Rules
    val stat = spark
      .read
      .format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("Statement")

    cus.show()
    acc.show()
    prodRule.show()
    stat.show()

    //Finding DTI:
    val stat1 = stat.withColumn("credited", when($"credited".isNull, 0).otherwise($"credited"))
    val statSumDeb = stat1.groupBy("accNo").agg(sum("debited").alias("totDebited"))
    val statSumCre = stat1.filter("source = 'salary'").groupBy("accNo").agg(sum("credited").alias("totCredited"))
    val statDebCre = statSumCre.join(statSumDeb, "accNo")
    val DTI_DF = statDebCre.withColumn("DTI", col("totDebited") / col("totCredited"))

    val DTI_DF_sal = DTI_DF.select("*").filter(DTI_DF("source") === "salary")

    // Final output
    def prodAndLoan(a: Double, b: Double): (String, Double) = if (a < 0.50) ("Car Loan", b*12*3) else if (a < 0.7) ("Personal loan", b * 12 * 5) else if (a < 0.3) ("Home loan", b*12*10) else ("No loan", 0)
    def myFunc: (Double, Double) => (String, Double) = prodAndLoan
    val prod_prop = udf(myFunc)

    val a = cus.join(DTI_DF, "accNo").select("accNo", "Name", "DTI")
    val b = a.withColumn("newCol", prod_prop(a("DTI"), lit(1000)))

    b.select($"accNo", $"Name", $"DTI", $"newCol.*").show()

    b.select($"accNo".alias("customer ACC"), $"Name", $"DTI", 
	concat(month(current_timestamp()),lit("-"),year(current_timestamp())).alias("Month"), 
	$"newCol._1".alias("loan"),$"newCol._2".alias("loanamt")).show()
  }
}
