package scala.bigdata

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object spark_udf {
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
    val df_tr = read_and_transform_type(spark)
    val df_tr_DF = df_tr.toDF("carat", "color", "clarity", "x", "y", "z", "depth", "arr1", "arr2").sample(0.01)
    // DataFrame to SQL table
    df_tr_DF.createOrReplaceTempView("diamonds")
    // 用spark SQL 运行SQL
    val sqlDF = spark.
      sql("select color,clarity,sum(x),max(x) from diamonds group by color,clarity")
    sqlDF.show()
    // 窗口函数练习
    val sqlDF1 = spark.
      sql(
        "select color,clarity,x," +
          " row_number() over " +
          "(partition by color order by x desc ) " +
          "as rank from diamonds")
    sqlDF1.show()


    // UDF
    // Register the UDF with our SparkSession
    // 注意 udf的作用相当于map操作，而不能做聚合，要做聚合要用UDAF
    spark.udf.register("myfun", (a: Double) => ((a * 9.0 / 5.0) + 32.0))
    spark.udf.register("strlen", (a: String) => a.length)
    spark.udf.register("two_cols_intersect", (a: Seq[String],b:Seq[String]) => a.intersect(b))


    val sqlUDF1 = spark.
      //sql("SELECT color,clarity,myfun(x),max(x) FROM diamonds group by color,clarity")
      sql("SELECT color,clarity,myfun(x) FROM diamonds")
   // sqlUDF1.show()
    val sqlUDF2 = spark.
      //sql("SELECT color,clarity,myfun(x),max(x) FROM diamonds group by color,clarity")
      sql("SELECT color,clarity,strlen(color) FROM diamonds")
  //  sqlUDF2.show()
    val sqlUDF3 = spark.
      //sql("SELECT color,clarity,myfun(x),max(x) FROM diamonds group by color,clarity")
      sql("SELECT color,clarity, two_cols_intersect(arr1,arr2) as inter FROM diamonds")
    sqlUDF3.show()
    // UDAF

    println("Run Successfully")
    spark.stop()

  }

  def read_and_transform_type(spark: SparkSession):
  Dataset[(Double, String, String, Double, Double, Double, String, Seq[Double], Seq[String])] = {
    // 函数的返回类型，要指定，DataFrame和Dataset不同
    val df = spark.read.option("header", "true").csv("./data/diamonds.csv")
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

}
