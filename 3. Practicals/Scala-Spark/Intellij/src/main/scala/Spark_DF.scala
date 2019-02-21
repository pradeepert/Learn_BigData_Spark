import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object Spark_DF {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val emp2 = spark.read.format("csv").option("header", "true").option("sep", ",").option("inferschema", "true").load("/home/nineleaps/Learn_BigData_Spark/3. Practicals/Scala-Spark/Intellij/dataset/emp.txt")
    val emp1 = emp2.withColumn("COMM", when(($"COMM" === lit("NULL")), 0).otherwise($"COMM"))
    val emp = emp1.withColumn("COMM", $"COMM".cast("double"))

    def toInt(df: org.apache.spark.sql.Column): org.apache.spark.sql.Column = {
      df.cast("double")
    }

    val dept = spark.read.format("csv").option("header", "true").option("sep", ",").option("inferschema", "true").load("/home/nineleaps/Learn_BigData_Spark/3. Practicals/Scala-Spark/Intellij/dataset/dept.txt")

    val salgrd = spark.read.option("header", "true").option("inferschema", "true").csv("/home/nineleaps/Learn_BigData_Spark/3. Practicals/Scala-Spark/Intellij/dataset/salgrade.txt")

    //-----------------------------------------------------------------------------------------------------------
    //DataFrame Various Practicals:

    //# 2. Display unique Jobs from EMP table?
    println("2. Display unique Jobs from EMP table?")
    emp.select($"JOB").distinct().show()
    //----------------------------------------------------------------------------------------------------------

    // 3. List the emps in the asc order of their Salaries?
    println("3. List the emps in the asc order of their Salaries?")
    emp.sort(asc("SAL")).show()
    // Or emp.sort($"SAL".asc).show()

    //----------------------------------------------------------------------------------------------------------

    // 4. List the details of the emps in asc order of the Dptnos and desc of Jobs?
    println("4. List the details of the emps in asc order of the Dptnos and desc of Jobs?")
    emp.sort($"DEPTNO".asc, $"JOB".desc).show()
    //----------------------------------------------------------------------------------------------------------

    // 5. Display all the unique job groups in the descending order?
    println("5. Display all the unique job groups in the descending order?")
    emp.select($"JOB").distinct().sort($"JOB".desc).show()
    //----------------------------------------------------------------------------------------------------------

    //Important
    //6. Display all the details of all ‘Mgrs’
    println("6. Display all the details of all 'Mgrs'")
    val eMgr = emp.select($"MGR").distinct().rdd.map(x => x(0)).collect()
    emp.filter($"EMPNO".isin(eMgr:_*)).show()
    //----------------------------------------------------------------------------------------------------------

    // 7. List the emps who joined before 1981.
    println("7. List the emps who joined before 1981.")
    emp.filter(year($"HIREDATE") < 1981).show()
    //----------------------------------------------------------------------------------------------------------

    // Find the name of the highest paied employee
    //Method 1: Using RDD

    println("Find the name of the highest paied employee, Method 1: Using RDD")
    val maxSal = emp.agg(max("SAL")).rdd.map(x=>x(0)).collect()
    emp.filter($"SAL".isin(maxSal:_*)).show()

    //Method 2: Using Rank
    println("Find the name of the highest paied employee, Method 1: Using Rank")
    val rank = emp.withColumn("rank", dense_rank().over(Window.orderBy($"SAL".desc)))
    rank.filter($"rank" === 1).select($"ENAME").show()
    //----------------------------------------------------------------------------------------------------------

    //# 8. List the Empno, Ename, Sal, Daily sal of all emps in the asc order of Annsal.
    println("8. List the Empno, Ename, Sal, Daily sal of all emps in the asc order of Annsal.")
    emp.select($"EMPNO", $"ENAME", $"SAL", ($"SAL"/30).alias("DAILYSAL"), ($"SAL"*12).alias("ANNSAL")).sort($"ANNSAL".asc).show()
    //----------------------------------------------------------------------------------------------------------

    //Important
    // # 9. Display the Empno, Ename, job, Hiredate, Exp of all Mgrs
    println("9. Display the Empno, Ename, job, Hiredate, Exp of all Mgrs")
    emp.filter($"EMPNO".isin(eMgr:_*)).select($"EMPNO", $"ENAME", $"JOB", $"HIREDATE", (year(current_date()) - year($"HIREDATE")).alias("EXP")).show()
    //----------------------------------------------------------------------------------------------------------

    // # 10.List the Empno, Ename, Sal, Exp of all emps working for Mgr 7839.
    println("10.List the Empno, Ename, Sal, Exp of all emps working for Mgr 7839.")
    emp.filter($"MGR" === 7839).select($"EMPNO", $"ENAME", $"SAL", (year(current_date()) - year($"HIREDATE")).alias("EXP")).show()
    //----------------------------------------------------------------------------------------------------------

    //# 11.Display all the details of the emps whose Comm. Is more than their Sal.
    println("11.Display all the details of the emps whose Comm. Is more than their Sal.")
    emp.filter($"COMM" > $"SAL").show()
    //----------------------------------------------------------------------------------------------------------

    //Important
    //# 12. List the emps in the asc order of Designations of those joined after the second half of 1981.
    println("12. List the emps in the asc order of Designations of those joined after the second half of 1981.")
    emp.filter(year($"HIREDATE") >= 1981 && month($"HIREDATE") > 6).sort(asc("JOB")).show()
    //----------------------------------------------------------------------------------------------------------

    //# 13. List the emps along with their Exp and Daily Sal is more than Rs.100.
    println("13. List the emps along with their Exp and Daily Sal is more than Rs.100.")
    emp.select($"*", (year(current_date()) - year($"HIREDATE")).alias("EXP"), ($"SAL"/30).alias("DailySal")).filter("DailySal > 100").show()
    //----------------------------------------------------------------------------------------------------------

    //# 14. List the emps who are either ‘CLERK’ or ‘ANALYST’ in the Desc order.
    println("14. List the emps who are either ‘CLERK’ or ‘ANALYST’ in the Desc order.")
    emp.filter($"JOB" === "CLERK" || $"JOB" === "ANALYST").show()
    //----------------------------------------------------------------------------------------------------------

    // # 15. List the emps who joined on 1-MAY-81,3-DEC-81,17-DEC-80,19-JAN-80 in asc order of seniority.
    println("15. List the emps who joined on 1-MAY-81,3-DEC-81,17-DEC-80,19-JAN-80 in asc order of seniority.")
    emp.filter($"HIREDATE".isin("1981-05-01 00:00:00","1981-12-03 00:00:00","1980-12-17 00:00:00","1980-01-19 00:00:00")).show()
    //----------------------------------------------------------------------------------------------------------

    //Important - UDF
    //If sal more than 1000 tell mgr else acc

    //Note: It will help to find the type
    def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

    println("If sal more than 1000 tell mgr else acc")
    def designation(a:Integer):String = {
      //print(manOf(a))
      if(a > 1000) {
        "MGR"
      }else {
        "ACC"
      }
    }

    val desi = udf[String,Integer](designation)
    emp.withColumn("Designation",desi($"SAL")).show()
    //----------------------------------------------------------------------------------------------------------

    //# 16. List the emp who are working for the Deptno 10 or20.
    println("16. List the emp who are working for the Deptno 10 or20.")
    emp.filter($"DEPTNO".isin(10,20)).show()
    //-----------------------------------------------------------------------------------------------------------

    //# 17. List the emps who are joined in the year 80.
    print("17. List the emps who are joined in the year 80.")
    emp.filter(year($"HIREDATE") === 1980).show()

    //# 18. List the emps who are joined in the month of Aug 1980.
    println("18. List the emps who are joined in the month of Aug 1980.")
    emp.filter(year($"HIREDATE") === 1980 && month($"HIREDATE") === 12).show()

    //IMPORTANT
    // #19. List the emps Who Annual sal ranging from 22000 and 45000.
    println("19. List the emps Who Annual sal ranging from 22000 and 45000.")
    emp.select($"*",($"SAL"*12).alias("AnnSal")).filter($"AnnSal".between(22000, 45000)).show()

    //#20. List the Enames those are having five characters in their Names.
    println("20. List the Enames those are having five characters in their Names.")
    emp.filter(length($"ENAME") === 5).show()
    
  }
}