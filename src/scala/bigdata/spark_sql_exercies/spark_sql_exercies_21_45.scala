package scala.bigdata.spark_sql_exercies

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._ // 包含了sql中使用的聚合函数

import org.apache.spark.sql.expressions.Window // 窗口函数

object spark_sql_exercies_21_45 {
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
    // 21 查询男生、女生人数

    println("第21题sql解法==============>")

    val s21_sql = spark.sql(
      """
        |select Ssex,count(Ssex) as nums
        |from Student
        |group by Ssex
        |""".stripMargin)

    s21_sql.show()
    println("第22题spark解法==============>")

    val s21_sp = Student_df.groupBy($"Ssex").count().as("nums")
    s21_sp.show()


    // 22 查询名字中含有「风」字的学生信息
    println("第22题sql解法==============>")

    val s22_sql =  spark.sql(
      """
        |select * from
        |Student
        |where Sname like "%风%"
        |""".stripMargin)

     s22_sql.show()

    println("第22题spark解法==============>")

    val s22_sp = Student_df.
      filter($"Sname" like "%风%")

      s22_sp.show()


 // 23 查询同名同性学生名单，并统计同名人数
    println("第23题sql解法==============>")

    val s23_sql = spark.sql(
      """
        |select Sname,count(SID) as nums
        |from Student
        |group by Sname
        |having nums >1
        |""".stripMargin)

        s23_sql.show()
    println("第23题spark解法==============>")

    val s23_sp = Student_df.
      groupBy($"Sname").
      agg(count($"SID").as("nums")).
      filter($"nums">1)
    s23_sp.show()

    // 24 查询 1990 年出生的学生名单
    println("第24题sql解法==============>")

    val s24_sql = spark.sql(
      """
        |select * from Student
        |where substring(Sage,0,4) = "1990"
        |""".stripMargin)

    s24_sql.show()
    println("第24题spark解法==============>")

    val s24_sp = Student_df.filter(substring($"Sage",0,4) === "1990")
    s24_sp.show()


  // 25 查询每门课程的平均成绩，结果按平均成绩降序排列，平均成绩相同时，按课程编号升序排列
    println("第25题sql解法==============>")
    val s25_sql = spark.sql(
      """
        |select t1.CID,t1.score_avg,
        |row_number() over(order by t1.score_avg desc) as rank1
        |from
        |(select CID,avg(score) as score_avg
        |from Score
        |group by CID) as t1
        |order by t1.score_avg desc,CID
        |""".stripMargin)

      s25_sql.show()
    println("第25题spark解法==============>")
    val Score_df_tmp = Score_df.
      groupBy($"CID").
      agg(mean($"score").as("score_avg"))

    val rankSpec1 = Window.orderBy(Score_df_tmp("score_avg").desc)
    val s25_sp = Score_df_tmp.
      withColumn("rank1", row_number.over(rankSpec1)).
      sort(desc("score_avg"),$"CID")
    s25_sp.show()



    // 26 查询平均成绩大于等于 60 的所有学生的学号、姓名和平均成绩
    println("第26题sql解法==============>")
    val s26_sql = spark.sql(
      """
        |select t1.SID,St.Sname,t1.score_avg
        |from
        |(select SID, avg(score) as score_avg
        |from Score
        |group by SID
        |having score_avg >=60) as t1
        |join Student St
        |on t1.SID = St.SID
        |""".stripMargin)


        s26_sql.show()

    println("第26题spark解法==============>")
      val s26_sp = Score_df.
        groupBy($"SID").
        agg(mean($"score").as("score_avg")).
        filter($"score_avg" >= 60).
        join(Student_df,Seq("SID")).
        select($"SID",$"Sname",$"score_avg")

    s26_sp.show()



    // 27  查询课程名称为「数学」，且分数低于 60 的学生姓名和分数
    println("第27题sql解法==============>")
    val s27_sql = spark.sql(
      """
        |select St.Sname,S.score
        |from Score S
        |join Student St
        |on S.SID = St.SID
        |where S.CID = (select CID from Course where Cname = "数学")
        |and S.score <60
        |""".stripMargin)

      s27_sql.show()

    println("第27题spark解法==============>")
    val CID_math = Course_df.
      filter($"Cname"==="数学").
      select($"CID").collect()(0)(0)

    // 通过array方法把值取出来，
    // 注意collect()方法返回的是scala的数组array，
    // 而collectAsList是Java的API(不过好像也能用，只是此时需要用get方法来获得值)

    println(CID_math)
      val s27_sp =Student_df.
        join(Score_df,Seq("SID"),joinType = "inner").
        filter($"CID"=== CID_math && $"score" <60).
        select($"Sname",$"score")
      s27_sp.show()

     // 28 查询所有学生的课程及分数情况（存在学生没成绩，没选课的情况）
    println("第28题sql解法==============>")
    val s28_sql = spark.sql(
      """
        |select St.Sname,St.SID,S.CID,S.score
        |from Student St
        |left join Score S
        |on St.SID = S.SID
        |""".stripMargin)

        s28_sql.show()

    println("第28题spark解法==============>")
    val s28_sp = Student_df.
      join(Score_df,Seq("SID"),joinType = "left").
      select($"Sname",$"SID",$"CID",$"score")
        s28_sp.show()

    //  29 查询任何一门课程成绩在 70 分以上的姓名、课程名称和分数
    println("第29题sql解法==============>")
      val s29_sql = spark.sql(
        """
          |select St.Sname,t1.SID,t1.CID,C.Cname,t1.score
          |from
          |(select SID,CID,score
          |from Score
          |where score >70) as t1
          |join Student St
          |on st.SID = t1.SID
          |join Course C
          |on C.CID = t1.CID
          |""".stripMargin)


 //       s29_sql.show()
    println("第29题spark解法==============>")

    val s29_sp = Score_df.
      filter($"score">70).
      join(Student_df,Seq("SID"),joinType = "inner").
      join(Course_df,Seq("CID"),joinType = "inner").
      select($"Sname",$"SID",$"CID",$"Cname",$"score")

   // s29_sp.show()

    // 30 查询不及格的课程
    println("第30题sql解法==============>")
    val s30_sql = spark.sql(
      """
        |select SID,CID,score
        |from Score
        |where score <60
        |""".stripMargin)

 //   s30_sql.show()
    println("第30题spark解法==============>")
    val s30_sp = Score_df.filter($"score"<60)
 //   s30_sp.show()

  // 31 查询课程编号为 01 且课程成绩在 40 分以上的学生的学号和姓名
    println("第31题sql解法==============>")
      val s31_sql = spark.sql(
        """
          |select Sname,SID
          |from
          |Student
          |where SID in
          |(select SID from Score
          |where CID = "01" and score >40)
          |""".stripMargin)

      s31_sql.show()


    println("第31题spark解法==============>")
      val s31_sp = Score_df.
        filter($"CID"==="01" && $"score">40).
        join(Student_df,Seq("SID"),joinType = "inner").
        select($"Sname",$"SID")

  //  s31_sp.show()

    // 32  求每门课程的学生人数
    println("第32题sql解法==============>")
    val s32_sql = spark.sql(
      """
        |select CID,count(distinct SID) as nums
        |from Score
        |group by CID
        |""".stripMargin)

      s32_sql.show()

    println("第32题spark解法==============>")
    val s32_sp = Score_df.groupBy($"CID").agg(countDistinct($"SID").as("nums"))
 //   s32_sp.show()

    // 33 成绩不重复，查询选修「张三」老师所授课程的学生中，成绩最高的学生信息及其成绩
    //  34 成绩有重复的情况下，查询选修「张三」老师所授课程的学生中，成绩最高的学生信息及其成绩
    // 这两道应该是窗口函数用rank()还是row_number()的区别
    println("第33题sql解法==============>")
    val s33_sql = spark.sql(
      """
        |with t1 as (select CID from
        |Course C
        |where TID =
        |(select TID from Teacher where Tname="张三")),
        |t2 as (
        |select S.CID,S.SID,S.score,
        |rank() over(partition by S.CID order by S.Score desc) as rank1
        |from Score S
        |join t1
        |on S.CID = t1.CID)
        |select t2.CID,St.SID,St.Sname,t2.score,t2.rank1
        |from
        |t2 join Student St
        |on St.SID = t2.SID
        |where t2.rank1 =1
        |""".stripMargin)

  //    s33_sql.show()

    println("第33题spark解法==============>")

    val s33_sp_t1 = Teacher_df.
      filter($"Tname"==="张三").
      select($"TID").
      join(Course_df,Seq("TID"),joinType = "inner").
      select($"CID").
      join(Score_df,Seq("CID"),joinType = "inner").
      select($"CID",$"SID",$"score")


    val rankSpec2 = Window.partitionBy("CID").orderBy(s33_sp_t1("score").desc)

    val s33_sp = s33_sp_t1.
      withColumn("rank1", rank.over(rankSpec2)).
      filter($"rank1"===1).
      join(Student_df,Seq("SID"),joinType = "inner").
      select($"CID",$"SID",$"Sname",$"score",$"rank1")

  //  s33_sp.show()

    println("第35题sql解法==============>")
    // 35 查询不同课程成绩相同的学生的学生编号、课程编号、学生成绩
    val s35_sql = spark.sql(
      """
        |select * from
        |(select SID,CID,score,
        |count(SID) over(partition by CID,score) as nums
        |from Score) as t1
        |where t1.nums > 1
        |order by t1.CID,t1.SID
        |""".stripMargin)

      s35_sql.show()

    println("第35题spark解法==============>")

    val rankSpec3 = Window.partitionBy("CID","score")
    val s35_sp = Score_df.
        withColumn("nums", count("SID").over(rankSpec3)).
        filter($"nums" > 1).
      sort($"CID",$"SID")

    s35_sp.show()

    // 36 查询每门功成绩最好的前两名
    println("第36题sql解法==============>")

    val s36_sql = spark.sql(
      """
        |select * from
        |(select CID,SID,score,
        |rank() over(partition by CID order by score) as rank1
        |from Score)as t1
        |where t1.rank1 <=2
        |""".stripMargin)

      s36_sql.show()

    println("第36题spark解法==============>")

    val rankSpec4 = Window.partitionBy("CID").orderBy(Score_df("score").desc)
    val s36_sp = Score_df.
      withColumn("rank1", rank.over(rankSpec4)).
      filter($"rank1" <= 2)
    s36_sp.show()

    //37 统计每门课程的学生选修人数（超过 5 人的课程才统计）。
    println("第37题sql解法==============>")

      val s37_sql =  spark.sql(
        """
          |select CID,count(distinct SID) as nums
          |from Score
          |group by CID
          |having nums >=5
          |""".stripMargin)

          s37_sql.show()

    println("第37题spark解法==============>")
     val s37_sp = Score_df.
       groupBy($"CID").
       agg(countDistinct($"SID").as("nums")).
       filter($"nums">=5)

    s37_sp.show()

    // 38 检索至少选修两门课程的学生学号
    println("第38题sql解法==============>")
    val s38_sql = spark.sql(
      """
        |select SID,count(distinct CID) as nums
        |from Score
        |group by SID
        |having nums>=2
        |""".stripMargin)

    s38_sql.show()
    println("第38题spark解法==============>")
    val s38_sp = Score_df.
      groupBy($"SID").
      agg(countDistinct($"CID").as("nums")).
      filter($"nums">=2)
    s38_sp.show()


    // 39 查询选修了全部课程的学生信息
    println("第39题sql解法==============>")
    val s39_sql = spark.sql(
      """
        |select SID,count(distinct CID) as nums
        |from Score
        |group by SID
        |having nums = (select count(distinct CID) from Score)
        |""".stripMargin)

    s39_sql.show()
    println("第39题spark解法==============>")
    // 可以直接对某一列进行聚合，此时相当于.groupby().agg(fun(col))
    val cid_all = Score_df.agg(countDistinct($"CID")).collect()(0)(0)
    val s39_sp = Score_df.groupBy($"SID").
      agg(countDistinct($"CID").as("nums")).filter($"nums" === cid_all)
    s39_sp.show()

    // 40-45是关于时间处理的
    // https://www.jianshu.com/p/61ae3e454af2
    // https://blog.csdn.net/lichangzai/article/details/19406215
    // https://blog.csdn.net/u013421629/article/details/80450047
    // 40 查询各学生的年龄，只按年份来算
    // 41 按照出生日期来算，当前月日 < 出生年月的月日则，年龄减一

    val s40_41_sql = spark.sql(
      """
        |select SID,
        |(year(current_date()) - year(Sage)) as age1
        |from Student
        |""".stripMargin)

   // |TIMESTAMPDIFF(YEAR,Sage,current_date()) as age2

    s40_41_sql.show()

    // 42 查询本周过生日的学生
    // 43 查询下周过生日的学生
    // 44 查询本月过生日的学生
    // 45 查询下月过生日的学生

    val s42_45_sql = spark.sql(
      """
        |select * from Student
        |where
        |weekofyear(Sage)=weekofyear(current_date())+1
        |or
        |weekofyear(Sage)=weekofyear(current_date())+1
        |or
        |MONTH(Sage)=MONTH(current_date())
        |or
        |MONTH(Sage)=MONTH(current_date())+1
        |""".stripMargin)

    s42_45_sql.show()
































    println("Run Successfully!")
    spark.stop()

  }


  // 分别创建 学生表Student;科目表Course;教师表Teachers;成绩表Score;
  def get_Student_table(spark: SparkSession): DataFrame = {
    val Student_arr: Array[(String, String, String, String)] =
      Array(("01", "赵雷", "1990-01-01", "男"),
        ("02", "赵雷", "1990-12-21", "男"),
        ("03", "孙小风", "1990-12-20", "男"),
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
