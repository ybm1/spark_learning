package scala.basic

import scala.math
// 函数定义和传参
object function_parameter {
  def main(args: Array[String]): Unit = {
    val x1 = 10.37
    val x2 = 8
    // 调用函数
    printf(x1 + "与" + x2 + "乘积的算法平方根为%.2f", mul_root(x1, x2))
    printf(x1 + "与" + x2 + "和的平方为：%.2f", sum_square(x1, x2))
    println("这些数字和的平方为：" + sum_square_new(10, 24, 12, 15, 33, 26, 15, 21, 27))
    println(mysqlConnect(user = "mysql", passwd = "mysql123"))
  }

  // 在main函数下面定义其他函数，在main中调用
  // 自定义两个数乘积的算法平方根
  def mul_root(x1: Double, x2: Double) = {
    val res = math.sqrt(x1 * x2)
    res // 最后一行默认是返回值
  }

  // 自定义两个数和的平方，其中p参数为默认参数
  def sum_square(x1: Double, x2: Double, p: Int = 2) = {
    val res = math.pow((x1 + x2), p)
    res
  }

  // 自定义含可变参数的函数
  def sum_square_new(args: Int*): Double = {
    var sum = 0
    for (i <- args) {
      sum += i
    }
    val res = math.pow(sum, 2)
    res
  }

  // 自定义函数 当自定义函数中既有必选参数，又有默认参数时，
  // 给函数传递的参数最好使用名称参数的方式，即parm_name = value。
  def mysqlConnect(host: String = "172.0.0.1", user: String,
                   passwd: String, port: Int = 3306) = {
    if (user == "mysql" && passwd == "mysql123") {
      println("恭喜您，数据库连接成功！")
    } else {
      println("对不起，用户名或密码错误！")
    }
  }
}