package scala.bigdata

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

object spark_word_count {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Demo").
      set("spark.default.parallelism", "3600").
      set("spark.sql.shuffle.partitions", "3600").
      set("spark.memory.fraction", "0.8").
      setMaster("local[*]")
    val spark = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    // 下面提供了 word count的10种做法，分为4类，以reduceByKey，groupBy，groupByKey和DataFrame操作分别作为主要方法

    val x = sc.parallelize(List("a b c", "b c d o", "a a", "a k", "b a", "b z","a"))
    // 下面是reduceByKey的做法，reduceByKey把相同Key的值放在一起然后做聚合，注意是直接做了聚合的，所以接受的是聚合函数
    // 即（x,y）代表两个不同key对应的value，而(x,y)=>x+y表示对其做加法
    // reduceByKey其实是在不同key组里做了reduce操作，
    // 关于reduce操作，参看：https://blog.csdn.net/guotong1988/article/details/50555671
    // reduce将RDD中元素前两个传给输入函数，产生一个新的return值，
    // 新产生的return值与RDD中下一个元素（第三个元素）组成两个元素，再被传给输入函数，直到最后只有一个值为止。
    // 也正是因为其直接对数据做了聚合，所以可控制较差，而groupByKey的可控制更好，因为groupByKey只做了分组没做聚合。
    // 之所以说reduceByKey的控制性弱，是因为，比如如果这里的(x, y) => x + y)改成(x, y) => x + 2*y)，
    // 那么把数据按照key分组后的顺序就非常重要，不同顺序最终结果不同，但是用reduceByKey无法操纵分组后的value的顺序，所以可控性弱
    // 但是reduceByKey的性能比groupByKey的性能要好

    val w1 = x.
      flatMap(a => a.split(" ")).
      map(a => (a, 1)).
      reduceByKey(_+_).
      sortBy(a => a._1)
    w1.collect()

    // 下面是groupByKey的做法，注意rdd经过了groupByKey的操作后，得到的是((k1,(v11,..,v1m)),(k2,(v21,..v2m))...)
    // 他把同一个Key对应的value都拉取到了，得到的是key-value的形式，所以后面一般会跟聚合操作，如直接对value进行操作的算子
    // mapValues,该算子直接对键值对中的value进行操作，这里把value转化为了Array，然后求了长度，这其实求得就是某个key出现的次数
    // 进而统计了某个词的出现次数
    val w2 = x.
      flatMap(a => a.split(" ")).
      map(a => (a, 1)).
      groupByKey().
      mapValues(_.toArray.length).sortBy(a=>a._1)
    w2.collect()
    // 下面是另一种基于groupByKey的做法，还是用完groupByKey后再用mapValues,除了求长度，还可以对键值对中的value求和，
    // 也可以达到同样目的
    val w3 = x.
      flatMap(a => a.split(" ")).
      map(a => (a, 1)).
      groupByKey()
      .mapValues(_.sum).sortBy(a=>a._1)
    w3.collect()
    // 下面是另一种基于groupByKey的做法，用map来代替mapValues的作用，既然groupByKey的返回的是键值对
    // 那么通过一个map可以把键取出来，然后对值求和或者求长度即可，下面两种分别是求和和求长度的做法
    // 在第二个map中求和
    val w4 = x.
      flatMap(a => a.split(" ")).
      map(a => (a, 1)).
      groupByKey().map(a=>(a._1,a._2.toArray.sum)).sortBy(a=>a._1)
    w4.collect()
    // 在第二个map中求长度
    val w5 = x.
      flatMap(a => a.split(" ")).
      map(a => (a, 1)).
      groupByKey().map(a=>(a._1,a._2.toArray.length)).sortBy(a=>a._1)
    w5.collect()
    // 下面是基于groupBy的操作，首先rdd经过按照Word进行groupBy之后，
    // 得到的是((k1,((k1,v11),...,(k1,v12))),(k2,((k2,v21),...,(k2,v22))),...)
    // 注意和groupByKey得到的不同，groupBy得到的也是键值对，
    // 它的键就是原来的键，而值却是以该键为键的所有进行groupBy之前的元素,在进行groupBy之前的元素是("a",1),("b",1)
    // 所以在进行groupBy之后，
    // 举例如"a"这个键对应的值就是(("a",1),...,("a",1)),值里面的元素都是上一个操作的输出元素，且键是"a"的
    // 所以在groupBy之后，可以直接mapvalues然后求值的长度即可,相当于对一个二维数组求长度，就是有多少个小数组，也可以满足需求
    // 注意这时候已经不能对值求和了
    val  w6= x.
      flatMap(a => a.split(" ")).
      map(a => (a, 1)).
      groupBy(a=>(a._1)).
      mapValues(_.toArray.length).sortBy(a=>a._1)
    w6.collect()
    // 根据上面mapValues和map的转化，也可以把mapValues用map来做，也还是求长度，不能求和
    val  w7= x.
      flatMap(a => a.split(" ")).
      map(a => (a, 1)).
      groupBy(a=>(a._1)).
      map(a=>(a._1,a._2.toArray.length)).sortBy(a=>a._1)
    w7.collect()
    // 其实wordcount无非就是一个最简单的分组求和问题，能不能像操作DataFrame那样用标准groupBy和聚合函数agg来做呢？
    // 可以把RDD转化为DataFrame，然后再groupBy并agg求和或者直接count即可，这可能是最容易接受的方法
    // 注意，要用agg算子，需要把RDD转化为DataFrame
    // 求和
    val  w8= x.
      flatMap(a => a.split(" ")).
      map(a => (a, 1)).toDF("words","nums").
      groupBy($"words").agg(sum($"nums").as("counts"))
    w8.show()
    // 直接count也可以
    val  w9= x.
      flatMap(a => a.split(" ")).
      map(a => (a, 1)).toDF("words","nums").
      groupBy($"words").count().sort("words")
    w9.show()
    // 还可以直接countByKey
    val  w10= x.
      flatMap(a => a.split(" ")).
      map(a => (a, 1)).
      countByKey().toArray
    w10


    //    val w6 = x.
    //      flatMap(a => a.split(" ")).
    //      map(a => (a, 1)).groupByKey().mapGroups((key,values) =>(key,values.length))
    //      .map(a=>(a._1,a._2.sum)).sortBy(a=>a._1)
    //    w6.collect()


  }

}
