package scala.bigdata.spark_sql_exercies

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._ // 包含了sql中使用的聚合函数

import org.apache.spark.sql.expressions.Window // 窗口函数

object spark_sql_exercies_11_20 {
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
    // 11 查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩

    println("第11题sql解法==============>")
    val s11_sql = spark.sql(
      """
        |select S.Sname as Sname,S.SID as SID,avg(Sc.score) as score_mean
        |from Student S join
        |(select SID,count(CID) as nums
        |from Score
        |where score <= 60
        |group by SID
        |having nums>=2) as t1
        |on S.SID = t1.SID
        |join Score Sc
        |on Sc.SID = S.SID
        |group by S.Sname,S.SID
        |order by S.Sname
        |""".stripMargin)

      s11_sql.show()

    println("第11题spark解法==============>")
      val s11_sp = Score_df.
        filter($"score"<=60).
        groupBy($"SID").
        agg(count($"CID").as("nums")).
        filter($"nums">=2).
        join(Score_df,Seq("SID"),joinType = "inner").
        join(Student_df,Seq("SID"),joinType = "inner").
        groupBy($"Sname",$"SID").
        agg(mean($"score")).
        sort($"Sname")

      s11_sp.show()

      // 12 检索" 01 "课程分数小于 60，按分数降序排列的学生信息
    println("第12题sql解法==============>")

    val s12_sql_1 = spark.sql(
      """
        |select SID,CID,score
        |from Score
        |where
        |SID in
        |(select SID
        |from Score
        |where CID ="01" and score <=60)
        |order by SID,score desc
        |""".stripMargin)

      s12_sql_1.show()

    val s12_sql_2 = spark.sql(
      """
        |select S.SID as SID,S.CID as CID,S.score as score
        |from Score S
        |right join
        |(select SID
        |from Score
        |where CID ="01" and score <=60) as t1
        |on S.SID = t1.SID
        |order by SID,score desc
        |""".stripMargin)
// 注意这里用join来代替了in，因为in往往会使执行速度变慢，把临时表作为右连接的右表(或者左连接的左表)即可

   s12_sql_2.show()


    println("第12题spark解法==============>")
    val s12_sp = Score_df.
      filter($"CID"==="01" && $"score" <=60).
      select($"SID").
      join(Score_df,Seq("SID"),joinType = "left").
      sort(asc("SID"),desc("score"))

    s12_sp.show()

// 13 按平均成绩从高到低显示所有学生的所有课程的成绩以及平均成绩
    println("第13题sql解法==============>")
    val s13_sql = spark.sql(
      """
        |select SID, avg(score) as score_avg
        |from Score
        |group by SID
        |order by score_avg desc
        |""".stripMargin)

     s13_sql.show()

    println("第13题spark解法==============>")

    val s13_sp = Score_df.
      groupBy($"SID").
      agg(mean($"score").as("score_avg")).
      sort(desc("score_avg"))

    s13_sp.show()

    //14 查询各科成绩最高分、最低分和平均分：
    //
    //以如下形式显示：课程 ID，课程 name，及格率，中等率，优良率，优秀率
    //
    //及格为>=60，中等为：70-80，优良为：80-90，优秀为：>=90
    //
    //要求输出课程号和选修人数，查询结果按人数降序排列，若人数相同，按课程号升序排列

    println("第14题sql解法==============>")
    val s14_sql = spark.sql(
      """
        |select C.Cname,S.CID,
        |count(case when S.score>=60 then 1 end)/count(S.CID) as jige_ratio,
        |count(case when S.score>=70 and S.score <80 then 1 end)/count(S.CID) as zhongdeng_ratio,
        |count(case when S.score>=80 and S.score <90 then 1 end)/count(S.CID) as youliang_ratio,
        |count(case when S.score>=90 then 1 end)/count(S.CID) as youxiu_ratio,
        |count(S.CID) as nums
        |from Score S join
        |Course C on S.CID=C.CID
        |group by C.Cname,S.CID
        |order by nums desc,CID
        |""".stripMargin)

      s14_sql.show()

    println("第14题spark解法==============>")
    val s14_sp = Score_df.
      join(Course_df,Seq("CID")).
      groupBy($"Cname",$"CID").
      agg((count(when($"score">=60,1).otherwise(null))/count($"CID")).as("jige_ratio"),
        (count(when($"score">=70 && $"score"<80 ,1).otherwise(null))/count($"CID")).as("zhongdeng_ratio"),
        (count(when($"score">=80 && $"score"<90 ,1).otherwise(null))/count($"CID")).as("youliang_ratio"),
        (count(when($"score">=90,1).otherwise(null))/count($"CID")).as("youxiu_ratio"),
        count($"CID").as("nums")).
      sort(desc("nums"),asc("CID"))

      s14_sp.show()

    //15 按各科成绩进行排序，并显示排名， Score 重复时保留名次空缺
    // 15.1 按各科成绩进行排序，并显示排名， Score 重复时合并名次
    println("第15题sql解法==============>")

      val s15_sql = spark.sql(
        """
          |select SID,CID,score,
          |rank() over(partition by CID order by score desc) as rank1,
          |dense_rank() over(partition by CID order by score desc) as rank2,
          |row_number() over(partition by CID order by score desc) as rank3
          |from Score
          |""".stripMargin)

     s15_sql.show()

    println("第15题spark解法==============>")
// spark 窗口函数 https://blog.csdn.net/fox64194167/article/details/80790754

    val rankSpec1 = Window.partitionBy("CID").orderBy(Score_df("score").desc)
    val s15_sp = Score_df.
      withColumn("rank1", rank.over(rankSpec1)).
      withColumn("rank2", dense_rank.over(rankSpec1)).
      withColumn("rank3", row_number.over(rankSpec1))
    s15_sp.show()

    //16 查询学生的总成绩，并进行排名，总分重复时保留名次空缺
    //16.1 查询学生的总成绩，并进行排名，总分重复时不保留名次空缺

    println("第16题sql解法==============>")

    val s16_sql = spark.sql(
      """
        |select S.SID,S.score_sum,
        |rank() over(order by S.score_sum desc) as rank1,
        |dense_rank() over(order by S.score_sum desc) as rank2,
        |row_number() over(order by S.score_sum desc) as rank3
        |from
        |(select SID,sum(score) as score_sum
        |from Score
        |group by SID) as S
        |""".stripMargin)

    s16_sql.show()

    println("第16题spark解法==============>")

    val Score_df_tmp = Score_df.groupBy($"SID").agg(sum($"score").as("score_sum"))
    val rankSpec2 = Window.orderBy(Score_df_tmp("score_sum").desc)
    val s16_sp = Score_df_tmp.
      withColumn("rank1", dense_rank.over(rankSpec2)).
      withColumn("rank2", rank.over(rankSpec2)).
      withColumn("rank3", row_number.over(rankSpec2))
    s16_sp.show()



    // 17 统计各科成绩各分数段人数：课程编号，课程名称，[100-85]，[85-70]，[70-60]，[60-0] 及所占百分比
    // 与 14题 几乎一样

    // 18 查询各科成绩前三名的记录
    println("第18题sql解法==============>")
  val s18_sql = spark.sql(
    """
      |select t1.SID,t1.CID,t1.score,t1.rank1 from
      |(select SID,CID,score,
      |rank() over (partition by CID order by score desc) as rank1
      |from Score) as t1
      |where t1.rank1 <=3
      |order by t1.CID
      |""".stripMargin)

    s18_sql.show()


    println("第18题spark解法==============>")
    val rankSpec3 = Window.partitionBy("CID").orderBy(Score_df("score").desc)
    val s18_sp = Score_df.
      withColumn("rank1", rank.over(rankSpec3)).
      filter($"rank1"<=3).
      sort("CID")

      s18_sp.show()

    //19 查询每门课程被选修的学生数
    println("第19题sql解法==============>")
    val s19_sql = spark.sql(
      """
        |select CID,count(distinct SID) as nums
        |from Score
        |group by CID
        |""".stripMargin)

    s19_sql.show()


    println("第19题spark解法==============>")
    val s19_sp = Score_df.groupBy($"CID").agg(countDistinct($"SID").as("nums"))
    s19_sp.show()


    // 20 查询出选修大于等于两门课程的学生学号和姓名
    println("第20题sql解法==============>")
     val s20_sql = spark.sql(
       """
         |select t1.SID,St.Sname,t1.nums
         |from
         |(select SID,count(score) as nums
         |from
         |Score
         |group by SID) as t1
         |join Student st on st.SID = t1.SID
         |where t1.nums >=2
         |""".stripMargin)

        s20_sql.show()
    println("第20题spark解法==============>")

    val s20_sp = Score_df.
      groupBy($"SID").
      agg(count($"score").as("nums")).
      join(Student_df,Seq("SID"),joinType = "inner").
      select($"SID",$"Sname",$"nums").
      filter($"nums" >= 2)
     s20_sp.show()


    println("Run Successfully!")
    spark.stop()


  }




  // 分别创建 学生表Student;科目表Course;教师表Teachers;成绩表Score;
  def get_Student_table(spark: SparkSession): DataFrame = {
    val Student_arr: Array[(String, String, String, String)] =
      Array(("01", "赵雷", "1990-05-01", "男"),
        ("02", "赵雷", "1990-08-21", "男"),
        ("03", "孙小风", "1990-06-20", "男"),
        ("04", "李云", "1990-12-06", "女"),
        ("05", "周大风", "1991-12-01", "女"),
        ("06", "吴兰", "1992-01-01", "女"),
        ("07", "吴兰", "1992-01-01", "女"))

    val Student = spark.createDataFrame(Student_arr).
      toDF("SID", "Sname", "Sage", "Ssex")
    Student
  }

  def get_Course_table(spark: SparkSession): DataFrame = {
    val Course_arr: Array[(String, String, String)] =
      Array(("01", "语文", "01"),
        ("02", "数学", "02"),
        ("03", "化学", "03"),
        ("04", "英语", "04"),
        ("05", "地理", "05"),
        ("06", "体育", "06"),
        ("07", "历史", "07"))

    val Course = spark.createDataFrame(Course_arr).
      toDF("CID", "Cname", "TID")
    Course
  }

  def get_Teacher_table(spark: SparkSession): DataFrame = {
    val Teacher_arr: Array[(String, String)] =
      Array(("01", "张三"),
        ("02", "李逵"),
        ("03", "李白"),
        ("04", "李商隐"),
        ("05", "司马懿"),
        ("06", "诸葛村夫"),
        ("07", "王朗"))

    val Teacher = spark.createDataFrame(Teacher_arr).
      toDF("TID", "Tname")
    Teacher
  }

  def get_Score_table(spark: SparkSession): DataFrame = {
    val Score_arr: Array[(String, String, Double)] =
      Array(
        ("01", "01", 30),
        ("01", "02", 30),
        ("01", "03", 30),
        ("01", "04", 28),
        ("01", "07", 28),

        ("02", "01", 38),
        ("02", "03", 45),
        ("02", "02", 93),
        ("02", "04", 82),
        ("02", "06", 69),

        ("03", "01", 28),
        ("03", "03", 40),
        ("03", "05", 70),
        ("03", "02", 28),
        ("03", "04", 40),
        ("04", "07", 30),

        ("05", "01", 10),
        ("05", "02", 38),
        ("05", "03", 89),
        ("05", "04", 42),
        ("05", "05", 10),
        ("05", "06", 38),
        ("05", "07", 29),


        ("06", "01", 65),
        ("06", "02", 69),
        ("06", "03", 70),
        ("06", "04", 55),
        ("06", "06", 69),
        ("06", "07", 70),

        ("07", "01", 70),
        ("07", "02", 69),
        ("07", "03", 70),
        ("07", "04", 55),
        ("07", "05", 70),
        ("07", "06", 69),
        ("07", "07", 70))

    val Score = spark.createDataFrame(Score_arr).
      toDF("SID", "CID", "score")
    Score
  }




}
