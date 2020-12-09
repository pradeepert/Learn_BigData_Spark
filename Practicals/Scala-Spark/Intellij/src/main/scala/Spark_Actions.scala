import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.Logger
import org.apache.log4j.Level


object Spark_Actions {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Temp Data").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //ACTIONS
    val x = sc.parallelize(1 to 10, 2)

    /*1. reduce(func)
    Aggregate the elements of the dataset using a function func (which takes two arguments and returns one). The function should be commutative and associative so that it can be computed correctly in parallel.*/
    val r = x.reduce((sum,n) => (sum + n))
    println(r)
    //OR
    //---------------------------------------------------------------------------------------------------------------
    //val y = x.reduce(_ + _)

   /* 2. collect()
    Return all the elements of the dataset as an array at the driver program. This is usually useful after a filter or other operation that returns a sufficiently small subset of the data.*/
    val clct = x.collect()
    //val xOut= x.glom().collect()
    //---------------------------------------------------------------------------------------------------------------

    /*3. count()
    Return the number of elements in the dataset.*/
    val cnt = x.count()
    //---------------------------------------------------------------------------------------------------------------

   /* 4. first()
    Return the first element of the dataset (similar to take(1)).*/
    val frst = x.first()
    //---------------------------------------------------------------------------------------------------------------

   /* 5. take(n)
    Return an array with the first n elements of the dataset.*/
    val tk = x.take(5)
    //---------------------------------------------------------------------------------------------------------------

   /* 6. takeSample(withReplacement, num, [seed])
    Return an array with a random sample of num elements of the dataset, with or without replacement, optionally pre-specifying a random number generator seed.*/
    val ts = x.takeSample(true, 3)
    //---------------------------------------------------------------------------------------------------------------

   /* 7. takeOrdered(n, [ordering])
    Return the first n elements of the RDD using either their natural order or a custom comparator.*/
    val to = x.takeOrdered(3)
    //---------------------------------------------------------------------------------------------------------------

    /*8. saveAsTextFile(path)
    Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem, HDFS or any other Hadoop-supported file system. Spark will call toString on each element to convert it to a line of text in the file.*/
    x.saveAsTextFile("/apps/log/results_01.txt")
    //---------------------------------------------------------------------------------------------------------------

   /* 9. saveAsSequenceFile(path) [(Java and Scala)]
    Write the elements of the dataset as a Hadoop SequenceFile in a given path in the local filesystem, HDFS or any other Hadoop-supported file system. This is available on RDDs of key-value pairs that implement Hadoop's Writable interface. In Scala, it is also available on types that are implicitly convertible to Writable (Spark includes conversions for basic types like Int, Double, String, etc).*/
   // x.saveAsSequenceFile("/apps/log/results_02.txt")
    //---------------------------------------------------------------------------------------------------------------

    /*10. saveAsObjectFile(path) [(Java and Scala)]
    Write the elements of the dataset in a simple format using Java serialization, which can then be loaded usingSparkContext.objectFile().*/
    x.saveAsObjectFile("/apps/log/results_03.txt")
    val y = sc.objectFile[Int] ("/apps/log/results_03.txt")
    y.collect
    //---------------------------------------------------------------------------------------------------------------

   /* 11. countByKey()
    Only available on RDDs of type (K, V). Returns a hashmap of (K, Int) pairs with the count of each key.*/
    val cbk = sc.parallelize(List((3, "Gnu"), (3, "Yak"), (5, "Mouse"), (3, "Dog")), 2)
    cbk.countByKey()
    //---------------------------------------------------------------------------------------------------------------

    /*12. foreach(func)
    Run a function func on each element of the dataset. This is usually done for side effects such as updating an Accumulator or interacting with external storage systems. */
    x.foreach(println)
  }
}