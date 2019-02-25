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
    //----------------------------------------------------------------------------------------------------------

    //# 18. List the emps who are joined in the month of Aug 1980.
    println("18. List the emps who are joined in the month of Aug 1980.")
    emp.filter(year($"HIREDATE") === 1980 && month($"HIREDATE") === 12).show()
    //----------------------------------------------------------------------------------------------------------

    //IMPORTANT
    // #19. List the emps Who Annual sal ranging from 22000 and 45000.
    println("19. List the emps Who Annual sal ranging from 22000 and 45000.")
    emp.select($"*",($"SAL"*12).alias("AnnSal")).filter($"AnnSal".between(22000, 45000)).show()
    //----------------------------------------------------------------------------------------------------------

    //#20. List the Enames those are having five characters in their Names.
    println("20. List the Enames those are having five characters in their Names.")
    emp.filter(length($"ENAME") === 5).show()
    //----------------------------------------------------------------------------------------------------------

    //#21. List the Enames those are starting with ‘S’ and with five characters.
    println("21. List the Enames those are starting with ‘S’ and with five characters")
    emp.filter(substring($"ENAME", 0,1) === "S").show()
    //----------------------------------------------------------------------------------------------------------

    // #22. List the emps those are having four chars and third character must be ‘r’.
    println("22. List the emps those are having four chars and third character must be ‘r’.")
    emp.filter(length($"ENAME") === 4).filter(substring($"ENAME", 3,1) === "R").show()
    //----------------------------------------------------------------------------------------------------------

    //IMPORTANT
    // # 29. List the emps who does not belong to Deptno 20.
    println("29. List the emps who does not belong to Deptno 20.")
    emp.filter(!($"DEPTNO" === 20)).show()
    //----------------------------------------------------------------------------------------------------------

    //# 30. List all the emps except ‘PRESIDENT’ & ‘MGR” in asc order of Salaries.
    println("30. List all the emps except ‘PRESIDENT’ & ‘MGR” in asc order of Salaries.")
    emp.filter(!($"JOB".isin("PRESIDENT","MGR"))).show()
    //----------------------------------------------------------------------------------------------------------

    //# 31. List all the emps who joined before or after 1981
    println("31. List all the emps who joined before or after 1981")
    emp.filter(!(year($"HIREDATE") === 1981)).show()
    //----------------------------------------------------------------------------------------------------------

    //IMPORTANT
    //# 32. List the emps whose Empno not starting with digit 78.
    println("32. List the emps whose Empno not starting with digit 78.")
    emp.filter(!($"EMPNO".like("78%"))).show()
    //----------------------------------------------------------------------------------------------------------

    // # 34. List the emps who joined in any year but not belongs to the month of April.
    println("34. List the emps who joined in any year but not belongs to the month of April.")
    emp.filter(!(month($"HIREDATE") === 4)).show()
    //----------------------------------------------------------------------------------------------------------

    // # 35. List all the Clerks of Deptno 20
    println("35. List all the Clerks of Deptno 20")
    emp.filter($"DEPTNO" === 20 && $"JOB" === "CLERK").show()
    //----------------------------------------------------------------------------------------------------------

    // #38. Display the location of SMITH.
    println("38. Display the location of SMITH.")
    emp.join(broadcast(dept), "DEPTNO").filter($"ENAME" === "SMITH").select($"ENAME", $"DLOC").show()
    //----------------------------------------------------------------------------------------------------------

    // #39. List the total information of EMP table along with DNAME and Loc of all the emps Working Under ‘ACCOUNTING’ & ‘RESEARCH’ in the asc Deptno.
    println("39. List the total information of EMP table along with DNAME and Loc of all the emps Working Under ‘ACCOUNTING’ & ‘RESEARCH’ in the asc Deptno.")
    emp.join(broadcast(dept), "DEPTNO").filter($"DNAME".isin("ACCOUNTING", "RESEARCH")).show()
    //----------------------------------------------------------------------------------------------------------

    // #40 List the Empno, Ename, Sal, Dname of all the ‘MGRS’ and ‘ANALYST’ working in New York, Dallas with an exp more than 7 years without receiving the Comm asc order of Loc.
    println("40 List the Empno, Ename, Sal, Dname of all the ‘MGRS’ and ‘ANALYST’ working in New York, Dallas with an exp more than 7 years without receiving the Comm asc order of Loc.")
    emp.join(broadcast(dept), "DEPTNO").filter($"JOB".isin("MANAGER","ANALYST") && $"DLOC".isin("NEW YORK", "DALLAS") && year(current_date()) - year($"HIREDATE") > 7 && $"COMM" === 0.0).show()
    //----------------------------------------------------------------------------------------------------------

    // #153. Find out all the emps who earn highest salary in each job type. Sort in descending salary order.
    println("153. Find out all the emps who earn highest salary in each job type. Sort in descending salary order.")
    emp.groupBy("JOB").agg(max("SAL") as "MaxSal").sort($"MaxSal".desc).show()
    //----------------------------------------------------------------------------------------------------------

    //Important
    //how to join two tables without primary key

    emp.join(salgrd, $"SAL".between($"losal", $"hisal")).show()

    //155 List the employee name,Salary and Deptno for each employee who earns a salary greater than the average for their department order by Deptno.
    println("155 List the employee name,Salary and Deptno for each employee who earns a salary greater than the average for their department order by Deptno.")

    val avgSal = emp.groupBy("DEPTNO").agg(avg("SAL") as "avgSal")
    emp.as("e").join(avgSal.as("a"), $"e.DEPTNO" === $"a.DEPTNO" && $"e.SAL" > $"a.avgSal").show()
    //----------------------------------------------------------------------------------------------------------

    // 156) List the Deptno where there are no emps.
    println("156) List the Deptno where there are no emps.")
    emp.join(dept, emp("DEPTNO") === dept("DEPTNO"), "right").filter($"EMPNO"isNull).show()
    //----------------------------------------------------------------------------------------------------------

    // 157) List the No.of emp’s and Avg salary within each department for each job.
    println("157. List the No.of emp’s and Avg salary within each department for each job.")
    emp.groupBy("DEPTNO","JOB").agg(count("EMPNO"),avg("SAL")).show()
    //----------------------------------------------------------------------------------------------------------

    // 158) Find the maximum average salary drawn for each job except for ‘President’.
    println("158) Find the maximum average salary drawn for each job except for ‘President’.")

    emp.filter($"JOB" !== "PRESIDENT").groupBy("JOB").agg(max("SAL")).show()
    emp.filter(!($"JOB".isin("PRESIDENT"))).groupBy("JOB").agg(max("SAL")).show()
    //----------------------------------------------------------------------------------------------------------

    // 159) Find the name and Job of the emps who earn Max salary and Commission.
    println("159) Find the name and Job of the emps who earn Max salary and Commission.")
    val maxSalry = emp.filter(!($"COMM" === 0)).agg(max($"SAL" + $"COMM"))
    maxSalry.show()
    //----------------------------------------------------------------------------------------------------------

    // 160) List the Name, Job and Salary of the emps who are not belonging to the department 10 but who have the same job and Salary as the emps of dept 10.
    println("160) List the Name, Job and Salary of the emps who are not belonging to the department 10 but who have the same job and Salary as the emps of dept 10.")
    val no10 = emp.filter($"DEPTNO" !== 10)
    val yes10 = emp.filter($"DEPTNO" === 10)

    no10.join(yes10, no10("JOB") === yes10("JOB") && no10("SAL") === yes10("SAL")).show()
    //----------------------------------------------------------------------------------------------------------

    // 162) List the Deptno, Name, Job, Salary and Sal+Comm of the emps who earn the second highest earnings (sal + comm.).
    println("162) List the Deptno, Name, Job, Salary and Sal+Comm of the emps who earn the second highest earnings (sal + comm.).")

    val w = Window.orderBy(($"SAL" + $"COMM").desc)
    val rank1 = emp.withColumn("rank", dense_rank().over(w))
    rank1.filter($"rank" === 2).show()

    emp.as("a").join(emp.as("b"), $"a.SAL" > $"b.SAL").select($"a.EMPNO", $"a.ENAME", $"a.JOB", $"a.MGR", $"a.SAL")

  }
}