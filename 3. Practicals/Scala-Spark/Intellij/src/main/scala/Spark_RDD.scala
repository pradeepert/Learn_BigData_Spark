import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Spark_RDD {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Temp Data").setMaster("local[*]")
    val sc = new SparkContext(conf)

   /* //Creating RDD Method 1
    val arr = 1 to 10
    val par_rdd = sc.parallelize(arr)
    println("Created RDD using Parallelize")
    par_rdd.foreach(println)*/

    /*//Creating RDD Method 2
    val make_rdd = sc.makeRDD(arr)
    println("Created RDD using make_rdd")
    make_rdd.foreach(println)
*/
    //Creating RDD Method 3
    val file_RDD = sc.textFile("/home/nineleaps/Learn_BigData_Spark/3. Practicals/Scala-Spark/Intellij/dataset/emp.txt")
    //file_RDD.foreach(println)
//--------------------------------------------------------------------------------------------------------------

  /* /* TRANSFORMATIONS
    1. map(func)
    Return a new distributed dataset formed by passing each element of the source through a function func.*/
    val map_rdd = make_rdd.map(line => (line, line.toString.length))
    map_rdd.foreach(println)*/
//--------------------------------------------------------------------------------------------------------------

   /* 2. filter(func)
    Return a new dataset formed by selecting those elements of the source on which func returns true.*/
    val filter_rdd = file_RDD.filter(line => line == "7566,JONES,MANAGER,7839,1981-04-02,2975.00,NULL,20")
    println("Filtered Value")
    filter_rdd.foreach(println)
//--------------------------------------------------------------------------------------------------------------
   /* 3. flatMap(func)
    Similar to map, but each input item can be mapped to 0 or more output items (so func should return a Seq rather than a single item).*/
    val flatMap_rdd = filter_rdd.flatMap(lines => lines.split(","))
    println("flatMap result")
    flatMap_rdd.foreach(println)
//--------------------------------------------------------------------------------------------------------------
   /* 4. mapPartitions(func)
    Similar to map, but runs separately on each partition (block) of the RDD, so func must be of typeIterator<T> => Iterator<U> when running on an RDD of type T.*/
    val mapPartitions_rdd = sc.parallelize(List(1,2,3,4,5,6,7,8), 2)

    println(": mapPartitions_rdd : ")
    mapPartitions_rdd.foreach(println)

    //Map:
    var sum = 0

    def sumFuncMap(numbers : Int) : Int =
    {
      return sum + numbers
    }

    //MapPartitions:
    sum = 0

    def sumFuncPartition(numbers : Iterator[Int]) : Iterator[Int] =
    {
      while(numbers.hasNext)
      {
        sum = sum + numbers.next()
      }
      return Iterator(sum)
    }

    println(": mapPartitions_rdd.map(sumFuncMap) : ")
    mapPartitions_rdd.map(sumFuncMap).foreach(println)

    println(": mapPartitions_rdd.mapPartitions(sumFuncPartition) : ")
    mapPartitions_rdd.mapPartitions(sumFuncPartition).foreach(println)
//--------------------------------------------------------------------------------------------------------------

   /* 5. mapPartitionsWithIndex(func)
    Similar to mapPartitions, but also provides func with an integer value representing the index ofthe partition, so func must be of type (Int, Iterator<T>) => Iterator<U> when running on an RDD of type T.*/
    val mpwi_rdd = sc.parallelize(1 to 9)

    mpwi_rdd.foreach(println)

    println(": mapPartitionsWithIndex : ")
    mpwi_rdd.mapPartitionsWithIndex( (index: Int, it: Iterator[Int]) => it.toList.map(x => index + ", "+x).iterator).foreach(println)
//--------------------------------------------------------------------------------------------------------------

    /*6. sample(withReplacement, fraction, seed)
    Sample a fraction of the data, with or without replacement, using a given random number generator seed.*/
    val s_rdd = sc.parallelize(1 to 9)
    s_rdd.sample(true,.2).foreach(println)
//--------------------------------------------------------------------------------------------------------------

   /* 7. union(otherDataset)
    Return a new dataset that contains the union of the elements in the source dataset and the argument.*/
    val u01_rdd = sc.parallelize(1 to 9)
    val u02_rdd = sc.parallelize(5 to 15)
    val union_rdd = u01_rdd.union(u02_rdd).collect
    union_rdd.foreach(println)
//--------------------------------------------------------------------------------------------------------------

   /* 8. intersection(otherDataset)
    Return a new RDD that contains the intersection of elements in the source dataset and the argument.*/
    val i01_rdd = sc.parallelize(1 to 9)
    val i02_rdd = sc.parallelize(5 to 15)
    val intersection_rdd = i01_rdd.intersection(i02_rdd).collect
    intersection_rdd.foreach(println)
//--------------------------------------------------------------------------------------------------------------

    /*9. distinct([numPartitions]))
    Return a new dataset that contains the distinct elements of the source dataset.*/
    val d01_rdd = sc.parallelize(1 to 9)
    val d02_rdd = sc.parallelize(5 to 15)
    val distinct_rdd = d01_rdd.union(d02_rdd).distinct.collect
    distinct_rdd.foreach(println)
//--------------------------------------------------------------------------------------------------------------

   /* 10. groupByKey([numPartitions])
    When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs.
    Note: If you are grouping in order to perform an aggregation (such as a sum or average) over each key,  using reduceByKey or aggregateByKey will yield much better performance.
      Note: By default, the level of parallelism in the output depends on the number of partitions of theparent RDD. You can pass an optional numPartitions argument to set a different number of tasks.*/
    val g_rdd = sc.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
    val gbk_rdd = g_rdd.groupByKey().collect()
    gbk_rdd.foreach(println)
//--------------------------------------------------------------------------------------------------------------

   /* 11. reduceByKey(func, [numPartitions])
    When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function func, which must be of type
    (V,V) => V. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.*/
    val r_arr = Array("one","two","two","four","five","six","six","eight","nine","ten")
    val rbk_rdd = sc.parallelize(r_arr).map(w => (w,1)).reduceByKey(_+_)
    //val rbk_rdd = sc.parallelize(r_arr).map(w => (w,1)).reduceByKey((v1,v2) => v1 + v2)

    rbk_rdd.foreach(println)
//--------------------------------------------------------------------------------------------------------------
   /* Difference Between groupbykey and reduceByKey:
      ReduceByKey aggregate all the values inside each partition first and then shuffle the data
        groupByKey aggreagte all the values after shuffling so it will take much time*/
    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))

    val wordCountsWithReduce = wordPairsRDD
      .reduceByKey(_ + _)
      .collect()

    val wordCountsWithGroup = wordPairsRDD
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .collect()
//--------------------------------------------------------------------------------------------------------------

    /*12. aggregateByKey(zeroValue)(seqOp, combOp, [numPartitions])
    When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and a neutral "zero" value.
    Allows an aggregated value type that is different than the input value type,
    while avoiding unnecessary allocations. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.*/

    val a_arr = Array("one","two","two","four","five","six","six","eight","nine","ten")
    val abk_rdd = sc.parallelize(a_arr).map(w => (w,1)).aggregateByKey(0)((k,v) => v+k, (k,v) => v+k).sortBy(_._2)

    abk_rdd.foreach(println)
//--------------------------------------------------------------------------------------------------------------

    /*13. sortByKey([ascending], [numPartitions])
    When called on a dataset of (K, V) pairs where K implements Ordered, returns a dataset of (K, V) pairssorted by keys in ascending or descending order, as specified in the boolean ascending argument.*/
    val s_arr = Array(("maths",52), ("english",75), ("science",82), ("computer",65), ("maths",85))
    val sbk_rdd = sc.parallelize(s_arr).sortByKey()

    sbk_rdd.foreach(println)
//--------------------------------------------------------------------------------------------------------------

   /* 14. join(otherDataset, [numPartitions])
    When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key. Outer joins are supported through leftOuterJoin, rightOuterJoin,and fullOuterJoin.*/
    val j01_data = sc.parallelize(Array(('A',1),('b',2),('c',3)))
    val j02_data = sc.parallelize(Array(('A',4),('A',6),('b',7),('c',3),('c',8)))
    val join_rdd = j01_data.join(j02_data)

    join_rdd.foreach(println)
//--------------------------------------------------------------------------------------------------------------

    /*15. cogroup(otherDataset, [numPartitions])
    When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (Iterable<V>, Iterable<W>)) tuples.This operation is also called groupWith.*/
    val cg01_data = sc.parallelize(Array(('A',1),('b',2),('c',3)))
    val cg02_data = sc.parallelize(Array(('A',4),('A',6),('b',7),('c',3),('c',8)))
    val cogroup_rdd = cg01_data.cogroup(cg02_data)

    cogroup_rdd.foreach(println)
//--------------------------------------------------------------------------------------------------------------
    /*16. cartesian(otherDataset)
    When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements).*/
    val c01_data = sc.parallelize(Array(('A',1),('b',2),('c',3)))
    val c02_data = sc.parallelize(Array(('A',4),('A',6),('b',7),('c',3),('c',8)))
    val cartesian_rdd = c01_data.cartesian(c02_data)

    cartesian_rdd.foreach(println)
//--------------------------------------------------------------------------------------------------------------
   /* 17. pipe(command, [envVars])
    Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script. RDD elements arewritten to the process's stdin and lines output to its stdout are returned as an RDD of strings.*/
    val p_data = List("hi","hello","how","are","you")
    val p_rdd = sc.makeRDD(p_data)

    //val scriptPath = "/apps/utility/test.ksh"

   /* $ cat /apps/utility/test.ksh
    #!/usr/bin/ksh

    echo "Running shell script"

    while read LINE
    do
      echo ${LINE}
    done


    //val pipe_rdd = p_rdd.pipe(scriptPath)
    //pipe_rdd.foreach(println)
    //Runtime.getRuntime.exec(Array(scriptPath))*/

//---------------------------------------------------------------------------------------------------------------

    /*18. coalesce(numPartitions)
    Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more efficiently after filtering down a large dataset.*/
    val c_rdd = sc.parallelize(Array("jan","feb","mar","april","may","jun"),3)
    val coalesce_rdd = c_rdd.coalesce(2)

    c_rdd.partitions.size
    coalesce_rdd.partitions.size

    coalesce_rdd.foreach(println)
//---------------------------------------------------------------------------------------------------------------
    /*19. repartition(numPartitions)
    Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them.
      This always shuffles all data over the network.*/
    val r_rdd = sc.parallelize(Array("jan","feb","mar","april","may","jun"),3)
    val repartition_rdd = r_rdd.repartition(5)

    r_rdd.partitions.size
    repartition_rdd.partitions.size

    repartition_rdd.foreach(println)
//---------------------------------------------------------------------------------------------------------------
   /*/* 20. repartitionAndSortWithinPartitions(partitioner)
    Repartition the RDD according to the given partitioner and, within each resulting partition, sort records bytheir keys.
    This is more efficient than calling repartition and then sorting within each partition because it can push the sorting down into the shuffle machinery.*/
    val randRDD = sc.parallelize(List( (2,"cat"), (6, "mouse"),(7, "cup"), (3, "book"), (4, "tv"), (1, "screen"), (5, "heater")), 3)

    val partitioned = randRDD.repartitionAndSortWithinPartitions(rPartitioner)

    def myfunc(index: Int, iter: Iterator[(Int, String)]) : Iterator[String] = {
      iter.map(x => "[partID:" +  index + ", val: " + x + "]")
    }

    partitioned.mapPartitionsWithIndex(myfunc).collect.foreach(println)*/

  }
}
