package scala.bigdata.spark_sql_exercies

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._ // 包含了sql中使用的聚合函数
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

     // s11_sql.show()

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

      //s11_sp.show()

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
        ("01", "01", 30),
        ("02", "01", 38),
        ("03", "01", 28),
        ("04", "02", 99),
        ("02", "03", 45),
        ("04", "01", 30),
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
        ("05", "01", 10),
        ("05", "02", 38),
        ("05", "03", 89),
        ("05", "04", 42),
        ("06", "04", 70),
        ("06", "03", 69),
        ("06", "02", 70),
        ("06", "01", 55))

    val Score = spark.createDataFrame(Score_arr).
      toDF("SID", "CID", "score")
    Score
  }


}
