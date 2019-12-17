package scala.bigdata

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object spark_learn {
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
    // 关于使用的数据可以用下面的方法创建测试用例
// 通过spark.createDataFrame可以把二维的Seq，Array,List等转化为DataFrame，可通过这样创建测试用例
    val tempDataFrame = spark.createDataFrame(
      Array(("1", "2", "3", "4", 0), ("1", "7", "8", "9", 1),
        ("2", "7", "8", "9", 1),("2", "7", "8", "9", 0),
        ("1", "7", "8", "9", 1),("3", "7", "8", "9", 1))
   ).toDF("a", "b","c","d","e")

// 这里用了一份宝石相关的数据来演示相关算子的操作
    val df_tr = read_and_transform_type(spark)
    df_tr.
      map(a=>(a._2,a._3,a._6)).
      groupByKey(a=>(a._3,a._2))
      //.groupBy(a=>(a._1,a._2))

    // 以树形结构展示数据的结果
    df_tr.printSchema()
    // 展示前20行
    df_tr.show
    // select  涉及到列名操作，先把Dataset 转化为DataFrame
    // 如果是Dataset，则不能通过列名取列，可以根据_.1来取
    val df_tr_DF = df_tr.toDF("carat", "color", "clarity", "x", "y", "z", "depth", "arr1", "arr2")
    df_tr_DF.select($"color", $"carat" + 1).show
    // filter
    df_tr_DF.filter($"carat" > 0.2).
      filter($"y".isin(0, 4)).show
    // groupby and agg,这里的groupby操作是对于DataFrame做的，是标准的分组聚合操作，

    df_tr_DF.groupBy($"color",$"depth").
          agg(count($"clarity").as("count"),sum($"x"-$"y"*2).as("sum"),mean($"y").as("mean")).
          show

    // groupBy (Dataset)
    val t = df_tr.
      map(t=>(t._2,t._3,t._5,t._6)).rdd.groupBy(a=>(a._1,a._2))
      //groupBy()
    t
    // reduceByKey
    val t1 = df_tr.
      map(t=>(t._2,t._3)).
    // ByKey类型的算子，都只能对键值对类型的数据进行操作，一般的Dataset不一定没有这种方法
    // 所以如果要做reduceByKey类型的操作，要先转为rdd这种的(k,v)对才可以
    // 也即意味着，reduceByKey的输入必须是(k,v)对，v可以是数可以是Array，
    // 但必须是(k,v)对，即每条输入数据的长度必须为2才可以，如果大于2，如上面的map是map(t=>(t._2,t._9,t._3))
    // 这样就无法使用reduceByKey了
      rdd.
    // reduceBykey操作接受了一个函数，该函数制定了对于(k,v)中的value做什么操作
    // 这里的操作就是把同一个key下的value递归地用"_"和"*"粘起来
      reduceByKey((x,y)=>x+"_"+y+"*").collect()
    // 这里又要涉及到reduce操作了，reduceByKey其实就是对同一个Key下面的value做reduce操作
    // reduce操作是两两元素递归地按照给定的函数去做
    // 关于reduce操作，参看：https://blog.csdn.net/guotong1988/article/details/50555671
    // reduceByKey直接做了两个事：分组和聚合
    // 而groupByKey则只做了一件事：分组，所以groupByKey后面一般会跟map,mapvalues等算子做聚合操作
    // 从这个角度看，reduceByKey灵活性比groupBykey较差

    // 下面再展示groupByKey的用法
    // 两种方式使用groupByKey:
    // 第一，以Dataset的方式，此时groupByKey里面要指定Key，groupByKey的单条数据的输入不必要是长度为2的
    // 此时返回的是((k1,((k1,v11),...,(k1,v12))),(k2,((k2,v21),...,(k2,v22))),...)，和groupBy的返回差不多

    val gbk_dataset = df_tr.
      map(t => (t._2, t._3,t._5)).
      groupByKey(a=>(a._1,a._2))
    gbk_dataset

    // 第二，以RDD的方式，此时groupByKey里面要指定Key，groupByKey的单条数据的输入必须是长度为2的，即必须是(k,v)
    // 且此时的groupByKey()里面不做设置，应该是默认以第一个为key,这时候的返回是((k1,(v11,..,v1m)),(k2,(v21,..v2m))...)
    //  他把同一个Key对应的value都拉取到了，得到的是key-value的形式，所以后面一般会跟聚合操作
    val gbk_rdd = df_tr.
      map(t => (t._2, t._6)).
      rdd.
      groupByKey().
      // 这里mapValues中的x即values,x.sum代表对values做了求和操作，这里要跟聚合操作
      mapValues(x=>x.sum)
    gbk_rdd

//
//    val gbk_df = df_tr.
//      map(t => (t._2, t._3,t._6,t._7)).
//      toDF("a","b","c","d").
//      groupByKey()
//    gbk_df

    //  注意rdd经过了groupByKey的操作后，得到的是((k1,(v11,..,v1m)),(k2,(v21,..v2m))...)
    // 他把同一个Key对应的value都拉取到了，得到的是key-value的形式，所以后面一般会跟聚合操作，如直接对value进行操作的算子
    // mapValues,该算子直接对键值对中的value进行操作,这里直接做了求和，相当于实现了分组求和的功能
    // 在进行mapValues之前要先做groupByKey把数据转化为键值对
      //mapValues(_.sum)

    // 对于flatMapValues: 同基本转换操作中的flatMap，只不过flatMapValues是针对[K,V]中的V值进行flatMap操作
    // 这里的value是Seq，做了groupByKey后再用flatMapValues把Seq拉平，再做了map操作
    val t13 = df_tr.
      map(t => (t._2, t._9)).
      rdd.
      groupByKey().flatMapValues(a=>a+"_")
      t13
    // flatMapGroups,MapGroups ？？ 未查到相关资料

    // flatMap
    val t2 = df_tr.flatMap(t => (t._9)).filter(a => a.contains("VV")).count()
    // flatMap相当于map+flatten操作，如果数据中某一列是Seq这种形式，即不是单个数或者字符(因为就一个元素没法拉平)，
    // 而是Seq，就可以根据需求来进行flatMap，
    // 上面的代码是对arr2这列中的Seq进行了flatMap，并统计了含有"VV"的元素的个数
    // 关于map和flapMap之间的区别，参考：https://blog.csdn.net/WYpersist/article/details/80220211
    println(t2)


    // DataFrame to SQL table
    df_tr_DF.createOrReplaceTempView("diamonds")

    val sqlDF = spark.
      sql("SELECT color,clarity,sum(x),max(x) FROM diamonds group by color,clarity")
    sqlDF.show()

    println("Run Successfully")
  }

  def read_and_transform_type(spark: SparkSession):
  Dataset[(Double, String, String, Double, Double, Double, String, Seq[Double], Seq[String])] = {
    // 函数的返回类型，要指定，DataFrame和Dataset不同
    val df = spark.read.option("header", "true").csv("./data/diamonds.csv")
//  补充对常见数据源的读取和保存  parquet文件
    // val usersDF = spark.read.parquet("examples/src/main/resources/users.parquet")
    //usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
    // json文件
//     val usersDF = spark.read.json("examples/src/main/resources/users.json")
//    usersDF.select("name", "favorite_color").write.save("namesAndFavColors.json")
// Hive表
    // spark.read.table(表名)，读取之后一般会用map做getAs来获取各个列
    // saveAsTable()把结果保存为表，insertInto()向表中插入某一分区的数据 如例行任务每天的数据插入
    import spark.implicits._
    val df_t = df.map(row => {
      val carat = row.getAs[String]("carat").toDouble
      val color = row.getAs[String]("color")
      val clarity = row.getAs[String]("clarity")
      val x = row.getAs[String]("x").toDouble
      val y = row.getAs[String]("y").toDouble
      val z = row.getAs[String]("z").toDouble
      val depth = row.getAs[String]("depth")
      val arr1 = Seq(x, y, z)
      val arr2 = Seq(color, clarity, depth)
      (carat, color, clarity, x, y, z, depth, arr1, arr2)
    })
    //.toDF("carat","color","clarity","x","y","z","depth","arr1","arr2")
    // 如果转化为DF，._1这种操作就不能使用，可以在最后转化为DF，中间的数据形式保持为Dataset
    // DF有列名，而Dataset没有，Dataset要通过 ._1,._2的方式取列
    df_t
  }
def wordcount(spark:SparkSession):RDD[(String,Int)] ={
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

  w1

}


}
