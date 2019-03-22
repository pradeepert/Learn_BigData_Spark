import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object DF_Importants {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val emp3 = spark.read.format("csv").option("header", "true").option("sep", ",").option("inferschema", "true").load("/home/nineleaps/Learn_BigData_Spark/3. Practicals/Scala-Spark/Intellij/dataset/emp.txt")
    val emp2 = emp3.withColumn("COMM", when(($"COMM" === lit("NULL")), null).otherwise($"COMM"))
    val emp1 = emp2.withColumn("MGR", when($"MGR" === lit("NULL"), null).otherwise($"MGR"))
    val emp0 = emp1.withColumn("COMM", $"COMM".cast("double"))
    val emp = emp0.withColumn("MGR", $"MGR".cast("integer"))

    def toInt(df: org.apache.spark.sql.Column): org.apache.spark.sql.Column = {
      df.cast("double")
    }

    val dept = spark.read.format("csv").option("header", "true").option("sep", ",").option("inferschema", "true").load("/home/nineleaps/Learn_BigData_Spark/3. Practicals/Scala-Spark/Intellij/dataset/dept.txt")

    val salgrd = spark.read.option("header", "true").option("inferschema", "true").csv("/home/nineleaps/Learn_BigData_Spark/3. Practicals/Scala-Spark/Intellij/dataset/salgrade.txt")


    //---------------------------------------------------------------------------------------------------------------------------
    println("EMP Details")
    emp.show()

    //---------------------------------------------------------------------------------------------------------------------------
    // Who is earning more than 2000 rs
    println("Person earning more than 2000")
    emp.filter(col("SAL") > 2000).show()

    //---------------------------------------------------------------------------------------------------------------------------
    //wants to remove the duplicates based on one column
    println("Duplicates removed based on particular column")
    emp.dropDuplicates("JOB").show()

    //---------------------------------------------------------------------------------------------------------------------------
    // How to Find the number of duplicate rows in spark
    println("Find the duplicate rows based on column")
    emp.groupBy("JOB").count().show()

    //---------------------------------------------------------------------------------------------------------------------------
    //remove the null values based on column
    println("Remove null values based on column")
    emp.filter(col("COMM").isNotNull).show()

    //if any column haves 'null' other than comm also will get dropped(but in this dataset we dont have like that values)
    println("if any row has null value entire row will get deleted")
    emp.na.drop(how = "any").show()
    //instead of "any" if I give "all" when all the row values are null then only it will get drop

    //---------------------------------------------------------------------------------------------------------------------------
    //Replace JOB value is CLERK by CLK
    println("Replaced Job of CLERK by CLK")
    emp.na.replace("JOB", Map("CLERK" -> "CLK")).show()

    //Replace wherever value is 7698 with 1234
    println("Replaced wherever the column has value 7698 with 1234")
    val empcls = emp.columns
    emp.na.replace(cols = Array(empcls:_*), Map(7698 -> 1234)).show()

    //---------------------------------------------------------------------------------------------------------------------------
    // Give name of the higehest salary getter
    println("highest Salary getter name")
    val HigSalName = emp.withColumn("RANK", dense_rank().over(Window.orderBy(desc("SAL")))).filter(col("RANK") === 1).select("ENAME").head().getString(0)
    println(HigSalName)
    println()

    //---------------------------------------------------------------------------------------------------------------------------
    //Finding second highest salary without ranking analytical function
    println("second highest salary without ranking analytical function")
    emp.filter(col("SAL") === emp.filter(col("SAL") < emp.select(max("SAL")).head().getDouble(0)).select(max("SAL")).head().getDouble(0)).show()
    //---------------------------------------------------------------------------------------------------------------------------
    //Passing multiple condition at a time in join

    // 38. Display the location of SMITH.

    emp.alias("e").join(dept.alias("d"),$"e.DEPTNO" === $"d.DEPTNO" && $"e.ENAME" === "SMITH").select("DLOC").show()
    //---------------------------------------------------------------------------------------------------------------------------

    val countByYear = emp.withColumn("year", year($"HIREDATE")).groupBy("year").agg(count("EMPNO"))

    val emptyDf = Seq.empty[(String, Int)].toDF("Years", "Total")

    val minX = countByYear.agg(min("year")).head.getInt(0)
    val maxX = countByYear.agg(max("year")).head.getInt(0)


    def factOfhh(e:DataFrame, by:Int): DataFrame = {
      var e = emptyDf
      var fyear = 1980
      val it = (countByYear.count()/by).toInt
      for (i <- 0 to it) {
        val firstSet = countByYear.filter($"year".between(fyear, fyear+by)).agg(sum("count(EMPNO)").alias("TOTAL")).withColumn("years", concat(lit(fyear), lit("_"), lit(fyear + by))).select("Years", "TOTAL")
        val finalSet:DataFrame = e.union(firstSet)
        e = finalSet
        fyear = fyear+by+1
      }
      return e
    }
    val res1 = factOfhh(emptyDf, 1)
    res1.show()
  }
}
