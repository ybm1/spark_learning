package scala.bigdata

import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object spark_sql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Demo").
      set("spark.default.parallelism", "3600").
      set("spark.sql.shuffle.partitions", "3600").
      set("spark.memory.fraction", "0.8").
      setMaster("local[*]")
    val spark = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val Student = get_Student_table(spark).createOrReplaceTempView("Student")
    val Course = get_Course_table(spark).createOrReplaceTempView("Course")
    val Teacher = get_Teacher_table(spark).createOrReplaceTempView("Teacher")
    val Score = get_Score_table(spark).createOrReplaceTempView("Score")

    // 1.查询"01"课程比"02"课程成绩高的学生的信息及课程分数
    // spark做法

    // SQL 做法



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
        ("05", "语文", "01"),
        ("06", "数学", "02"),
        ("07", "物理", "01"),
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
      Array(("01", "宋江"),
        ("02", "李逵"),
        ("03", "鲁智深"),
        ("04", "武松"),
        ("05", "卢俊义"),
        ("06", "司马懿"),
        ("07", "诸葛亮"),
        ("09", "周瑜"),
        ("10", "曹操"),
        ("11", "关羽"),
        ("12", "张飞"),
        ("13", "赵云"))

    val Teacher = spark.createDataFrame(Teacher_arr).
      toDF("TID", "Tname")
    Teacher
  }
  def get_Score_table(spark: SparkSession): DataFrame = {
    val Score_arr: Array[(String,String, Double)] =
      Array(("01", "02",90),
        ("02","01" ,88),
        ("03", "01",78),
        ("04", "02",99),
        ("02","03" ,65),
        ("04","01" ,70),
        ("02","02" ,93),
        ("01", "03",88),
        ("01","01" ,89),
        ("02","02" ,82),
        ("03","03" ,70),
        ("02","04" ,69))

    val Score = spark.createDataFrame(Score_arr).
      toDF("SID","CID", "score")
    Score
  }


}
