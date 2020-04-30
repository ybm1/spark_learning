package scala.bigdata.spark_sql_exercies

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._ // 包含了sql中使用的聚合函数
object spark_sql_exercies_1_10 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Demo").
      set("spark.default.parallelism", "3600").
      set("spark.sql.shuffle.partitions", "3600").
      set("spark.memory.fraction", "0.8").
      setMaster("local[*]")
    val spark = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._ // 别忘了加上

    val Student = get_Student_table(spark).createOrReplaceTempView("Student")
    val Course = get_Course_table(spark).createOrReplaceTempView("Course")
    val Teacher = get_Teacher_table(spark).createOrReplaceTempView("Teacher")
    val Score = get_Score_table(spark).createOrReplaceTempView("Score")


    val Student_df = get_Student_table(spark)
    val Course_df = get_Course_table(spark)
    val Teacher_df = get_Teacher_table(spark)
    val Score_df = get_Score_table(spark)

    // 1.查询"01"课程比"02"课程成绩高的学生的信息及课程分数
    // SQL 做法  关键点是自连接
    println("第1题sql解法==============>")
    val s1_sql = spark.
      sql("select st.Sname,s1.SID,s1.CID,s1.score,s2.CID as CID2,s2.score as score2  " +
        " from Score s1, Score s2,Student st " +
        "where s1.SID=s2.SID and st.SID = s1.SID " +
        " and s1.CID='01' and s2.CID='02' " +
        "and  s1.score>=s2.score")
    val s1_sql_1 = spark.
      sql("select st.Sname,s1.SID,s1.CID,s1.score,s2.CID as CID2,s2.score as score2 " +
        "from Score s1 join Score s2 on s1.SID=s2.SID " +
        "join Student st on st.SID=s2.SID " +
        "where s1.CID='01' and s2.CID='02' " +
        "and s1.score>=s2.score"
      )


     s1_sql.show()
     s1_sql_1.show()

    val Score2 = Score_df.
      //注意这里的重命名列的方法
      withColumnRenamed("CID", "CID2").
      withColumnRenamed("score", "score2")
    println("第1题spark解法==============>")
    val s1_sp = Score_df.
      join(Score2, Seq("SID"), joinType = "inner").
      join(Student_df, Seq("SID"), joinType = "inner").
      // 注意是三等号
      filter($"CID" === "01" && $"CID2" === "02").
      filter($"score" > $"score2").
      select("Sname", "SID", "CID", "score", "CID2", "score2")
      s1_sp.show()

    println("第1.1题sql解法==============>")
    //  1.1 查询同时存在" 01 "课程和" 02 "课程的情况

    val s1_1_sql = spark.sql(
      """
        |select st.Sname,st.SID,count(distinct sc.CID) as nums
        |from Student st join Score sc on st.SID = sc.SID
        |where (CID = "01" or CID = "02")
        |group by st.Sname,st.SID
        |having nums >= 2
        |order by Sname,SID
        |""".stripMargin)
     s1_1_sql.show()
    println("第1.1题spark解法==============>")
    val s1_1_sp = Student_df.
      join(Score_df, Seq("SID"), joinType = "inner").
      filter($"CID" === "01" || $"CID" === "02").
      groupBy($"Sname", $"SID").agg(countDistinct($"CID").as("nums")).
      filter($"nums" >= 2).
      sort($"Sname", $"SID")
      s1_1_sp.show()

    println("第2题sql解法==============>")
    // 2.查询平均成绩大于等于 60 分的同学的学生编号和学生姓名和平均成绩
    val s2_sql = spark.sql(
      """
         select st.Sname,st.SID,avg(sc.Score) as avg_score
         from Student st join Score sc on st.SID = sc.SID
         group by st.Sname,st.SID
         having avg_score >= 60
         """)

       s2_sql.show()
    println("第2题spark解法==============>")
    val s2_sp = Student_df.
      join(Score_df, Seq("SID"), joinType = "inner").
      groupBy($"Sname", $"SID").
      agg(mean($"score").as("avg_score")).
      filter($"avg_score" >= 60)
      s2_sp.show()


    // 3 查询在 SC 表存在成绩的学生信息
    // 没看懂，直接筛选成绩非空的？

    // 4 查询所有同学的学生编号、学生姓名、选课总数、所有课程的总成绩(没成绩的显示为 null )
    println("第4题sql解法==============>")
    val s4_sql = spark.sql(
      """
        select st.Sname,st.SID,count(sc.CID) as nums, sum(sc.score) as score_sum
        from Student st join Score sc on st.SID = sc.SID
        group by st.Sname,st.SID
        """)
     s4_sql.show()
    println("第4题spark解法==============>")
    val s4_sp = Student_df.
      join(Score_df, Seq("SID"), joinType = "inner").
      groupBy($"Sname", $"SID").
      agg(count($"CID").as("nums"), sum($"score").as("score_sum"))
      s4_sp.show()

    // 4.1 查有成绩的学生信息
    // 没看懂，直接筛选成绩非空的？

    // 5 查询「李」姓老师的数量
    println("第5题sql解法==============>")
    val s5_sql = spark.sql(
      """
        |select count(*) as nums
        |from Teacher
        |where Tname like "李%"
        |""".stripMargin)

     s5_sql.show()
    println("第5题spark解法==============>")
    val s5_sp = Teacher_df.
      filter($"Tname" like "李%").
      // agg算子可以直接聚合,也可以group by之后再聚合
      agg(count($"Tname").as("nums"))

    s5_sp.show()

    // 6 查询学过「李逵」老师授课的同学的信息
    println("第6题sql解法==============>")
    val s6_sql = spark.sql(
      """
        |select st.SID as SID, c.CID as CID,t.TID as TID,
        |t.Tname as Tname, c.Cname as Cname, sc.score as score,
        |st.Sname as Sname,st.Sage as Sage,st.Ssex as Ssex
        |from Teacher t join Course c on t.TID = c.TID
        |join Score sc on sc.CID = c.CID
        |join Student st on st.SID = sc.SID
        |where t.Tname = "李逵"
        |""".stripMargin)
    s6_sql.show()
    println("第6题spark解法==============>")
    val s6_sp = Teacher_df.
      join(Course_df, Seq("TID")).
      join(Score_df, Seq("CID")).
      join(Student_df, Seq("SID")).
      filter($"Tname" === "李逵")
    s6_sp.show()

    // 7 查询没有学全所有课程的同学的信息
    // 关于嵌套查询，见：https://www.cnblogs.com/glassysky/p/11559082.html
    println("第7题sql解法==============>")
    val s7_sql = spark.sql(
      """
       select st.Sname,st.SID,count(distinct sc.CID) as nums
       from Student st
       join Score sc on st.SID = sc.SID
       group by st.Sname,st.SID
       having nums <
       (select count(distinct sc.CID) from
       Score sc)
       """.stripMargin)
    s7_sql.show()
    println("第7题spark解法==============>")
    val all_cids = Score_df.agg(countDistinct($"CID").as("al")).collectAsList()
    //println(all_cids)
    //println(all_cids.toArray())
    val s7_sp = Student_df.
      join(Score_df, Seq("SID")).
      groupBy($"Sname", $"SID").
      agg(countDistinct($"CID").as("nums")).
      filter($"nums" < all_cids.get(0)(0))
    s7_sp.show()


    // 8 查询至少有一门课与学号为" 01 "的同学所学相同的同学的信息
    spark.udf.register("two_cols_intersect", (a: Seq[String], b: Seq[String]) => a.intersect(b))
    spark.udf.register("arr_len", (a: Seq[String]) => a.toArray.length)
    println("第8题sql解法==============>")
    val s8_sql = spark.sql(
      """
        |with t1 as (
        |select collect_set(S1.CID) as c_1
        | from Score S1
        | where S1.SID = "01"
        | group by
        | S1.SID),
        |t2 as (
        |select S.SID,collect_set(S.CID) as c_all
        |from Score as S
        |group by
        |S.SID
        |)
        |select t3.SID as SID,t3.nums as nums
        |from
        |(select t2.SID as SID,
        |arr_len(two_cols_intersect(t2.c_all,(select t1.c_1 from t1))) as nums
        |from t2) as t3
        |where nums >=1 and SID != "01"
        |order by SID
        |""".stripMargin)
      s8_sql.show()

       val t1_c1 = Score_df.
      filter($"SID" === "01").
      groupBy($"SID").
      agg(collect_set($"CID").as("c1")).
      //select($"c1").
      map(a=> a.getAs[Seq[String]]("c1").toArray).
      collectAsList(). // 这个以及下面的get方法非常重要，可以获得一个具体的东西
      get(0)
   // println(t1_c1.mkString(","))
    println("第8题spark解法==============>")
    val s8_sp = Score_df.
      groupBy($"SID").
      agg(collect_set($"CID").as("c_all")).
      map(a => {
        val sid = a.getAs[String]("SID")
        val c_all = a.getAs[Seq[String]]("c_all").
          toArray.intersect(t1_c1).length
        (sid, c_all)
      }).
      toDF("SID", "nums").
      // 注意 === 和=!=是Column类中定义的新函数
      filter($"SID" =!= "01" && $"nums" >= 1).
      sort($"SID")
    //s8_sp.take(3).foreach(println)
    s8_sp.show()


    // 9 查询和" 01 "号的同学学习的课程 完全相同的其他同学的信息
    println("第9题sql解法==============>")
    val s9_sql = spark.sql(
      """
        |with t1 as (
        |select collect_set(S1.CID) as c_1
        | from Score S1
        | where S1.SID = "01"
        | group by
        | S1.SID),
        |t2 as (
        |select S.SID,collect_set(S.CID) as c_all
        |from Score as S
        |group by
        |S.SID
        |)
        |select t3.SID as SID,t3.nums as nums
        |from
        |(select t2.SID as SID,
        |arr_len(two_cols_intersect(t2.c_all,(select t1.c_1 from t1))) as nums
        |from t2) as t3
        |where nums = (select arr_len(c_1) from t1)
        | and SID != "01"
        |order by SID
        |""".stripMargin)
    s9_sql.show()
    println("第9题spark解法==============>")
    val t1_c1_s = Score_df.
      filter($"SID" === "01").
      groupBy($"SID").
      agg(collect_set($"CID").as("c1")).
      //select($"c1").
      map(a=> a.getAs[Seq[String]]("c1").toArray).
      collectAsList(). // 这个以及下面的get方法非常重要，可以获得一个具体的东西
      get(0)
    val c1_l = t1_c1_s.length
    //println(t1_c1_s.mkString(","))
    val s9_sp = Score_df.
      groupBy($"SID").
      agg(collect_set($"CID").as("c_all")).
      map(a => {
        val sid = a.getAs[String]("SID")
        val c_all = a.getAs[Seq[String]]("c_all").
          toArray.intersect(t1_c1_s).length
        (sid, c_all)
      }).
      toDF("SID", "nums").
      // 注意 === 和=!=是Column类中定义的新函数
      filter($"SID" =!= "01" && $"nums" === c1_l).
      sort($"SID")

    s9_sp.show()

// 10 查询没学过"张三"老师讲授的任一门课程的学生姓名
    println("第10题sql解法==============>")
    val s10_sql = spark.sql(
      """
        |with t1 as (
        |select collect_set(C.CID) as c_1
        |from
        |Course C join Teacher T on
        |C.TID = T.TID
        |where T.Tname = "张三"
        |group by
        |T.TID),
        |t2 as (
        |select S.SID,collect_set(S.CID) as c_all
        |from Score as S
        |group by
        |S.SID
        |)
        |select S.Sname as Sname,nums
        |from
        |(select t2.SID as SID,
        |arr_len(two_cols_intersect(t2.c_all,(select t1.c_1 from t1))) as nums
        |from t2) as t3 join Student S
        |on t3.SID = S.SID
        |order by Sname
        |""".stripMargin)

    s10_sql.show()
    println("第10题spark解法==============>")
    val t1_c1_t = Course_df.
      join(Teacher_df,Seq("TID")).
      filter($"Tname" === "张三").
      groupBy($"TID").
      agg(collect_set($"CID").as("c1")).
      map(a=> a.getAs[Seq[String]]("c1").toArray).
      collectAsList(). // 这个以及下面的get方法非常重要，可以获得一个具体的东西
      get(0)
    //println(t1_c1_t.mkString(","))
    val s10_sp = Score_df.
      groupBy($"SID").
      agg(collect_set($"CID").as("c_all")).
      map(a => {
        val sid = a.getAs[String]("SID")
        val c_all = a.getAs[Seq[String]]("c_all").
          toArray.intersect(t1_c1_t).length
        (sid, c_all)
      }).
      toDF("SID", "nums").
      join(Student_df,Seq("SID")).
      select($"Sname",$"nums").
      sort($"Sname")

 s10_sp.show()




    println("Run Successfully!")
    spark.stop()
  }


  // 分别创建 学生表Student;科目表Course;教师表Teachers;成绩表Score;
  def get_Student_table(spark: SparkSession): DataFrame = {
    val Student_arr: Array[(String, String, String, String)] =
      Array(("01", "赵雷", "1990-01-01", "男"),
        ("02", "钱电", "1990-12-21", "男"),
        ("03", "孙风", "1990-12-20", "男"),
        ("04", "李云", "1990-12-06", "男"),
        ("05", "周梅", "1991-12-01", "女"),
        ("06", "吴兰", "1992-01-01", "女"),
        ("07", "郑竹", "1989-01-01", "女"),
        ("09", "张三", "2017-12-20", "女"),
        ("10", "李四", "2017-12-25", "女"),
        ("11", "李四", "2012-06-06", "女"),
        ("12", "赵六", "2013-06-13", "女"),
        ("13", "孙七", "2014-06-01", "女"))

    val Student = spark.createDataFrame(Student_arr).
      toDF("SID", "Sname", "Sage", "Ssex")
    Student
  }

  def get_Course_table(spark: SparkSession): DataFrame = {
    val Course_arr: Array[(String, String, String)] =
      Array(("01", "语文", "02"),
        ("02", "数学", "02"),
        ("03", "数学", "03"),
        ("04", "英语", "02"),
        ("02", "语文", "01"),
        ("06", "数学", "02"),
        ("04", "物理", "01"),
        ("09", "语文", "02"),
        ("10", "英语", "04"),
        ("11", "英语", "03"),
        ("12", "英语", "05"),
        ("13", "数学", "03"))

    val Course = spark.createDataFrame(Course_arr).
      toDF("CID", "Cname", "TID")
    Course
  }

  def get_Teacher_table(spark: SparkSession): DataFrame = {
    val Teacher_arr: Array[(String, String)] =
      Array(("01", "张三"),
        ("02", "李逵"),
        ("03", "鲁智深"),
        ("04", "武松"),
        ("05", "卢俊义"))

    val Teacher = spark.createDataFrame(Teacher_arr).
      toDF("TID", "Tname")
    Teacher
  }

  def get_Score_table(spark: SparkSession): DataFrame = {
    val Score_arr: Array[(String, String, Double)] =
      Array(("01", "02", 30),
        ("02", "01", 38),
        ("03", "01", 28),
        ("04", "02", 99),
        ("02", "03", 45),
        ("04", "01", 70),
        ("02", "02", 93),
        ("02", "02", 82),
        ("03", "03", 40),
        ("02", "04", 69),
        ("02", "08", 70),
        ("02", "07", 93),
        ("01", "07", 28),
        ("02", "06", 82),
        ("03", "05", 70),
        ("02", "07", 69),
        ("05", "01", 93),
        ("05", "02", 38),
        ("05", "03", 89),
        ("05", "04", 42),
        ("06", "04", 70),
        ("06", "03", 69),
        ("06", "02", 70),
        ("06", "01", 69))

    val Score = spark.createDataFrame(Score_arr).
      toDF("SID", "CID", "score")
    Score
  }


}
