package scala.basic

import scala.io.StdIn.readInt
import scala.io.StdIn
import scala.util.control.Breaks._
// 基本运算和控制流
object scala_basic {
  def main(args: Array[String]): Unit = {
    // 基本运算
    val  a = 23
    val  b = 6
    println("a="+a + " ,b="+b) // 打印a和b的值
    println("a + b =" + (a + b)) // 加法运算
    println("a - b =" + (a - b)) // 减法运算
    println("a * b =" + (a * b)) // 乘法运算
    println("a / b =" + (a / b)) // 除法运算（两个整数的商）
    println("a * 1.0 / b =" + (a*1.0 / b)) // 除法运算（浮点数与整数的商）
    println("a % b =" + (a % b)) // 余数运算

    var c = a
    c += 10 // 自加运算
    println("a += 10 = " + c) // 式子中的加号为字符串的拼接运算

    // 输入和输出
    // 方法一：等号赋值法
    val name : String = "JJ"

    // 方法二：函数输入法
    println("请输入您的年龄：")
    val age = readInt()  // 输入不同数据类型的值，所使用的函数不一样

    println("亲爱的" + name + "先生，您的年龄为" + age + "岁")

//    a) println，结合加号（+），将多个内容进行连接并打印输出
//    b) println，传入表达式（表达式中所涉及的变量必须以美元符号作前缀），
//    其中表达式须用大括号框起来，同时需要在被打印对象的最前面加上字母s
//    c) printf，格式化输出，类似于Python的用法

    val balance = 6.52

    // 加号（+）拼接法
    println("亲爱的" + name + "先生，您的话费余额为" + balance + "元。")
    // s表达式法
    println(s"亲爱的${name}先生，您的话费余额为${balance}元")
    // 格式化输出法
    printf("亲爱的%s先生，您的话费余额为%.2f元\n", name, balance)
// 控制流
    println("请输入月份：")
    val month = StdIn.readInt()
    println("请问是否为学生：")
    val is_student = StdIn.readLine()
    if (month >= 3 && month <= 10){
      if (is_student == "否") {
        println("您的票价是150元")
      } else {
        println("您的票价是80元")
      }
    }
    else {
      if (is_student == "否") {
        println("您的票价是100元")
      } else {
        println("您的票价是50元")
      }
    }
// for循环
    var counts = 0
    var sum = 0
    for (i <- 2 to 100 by 2) {
      if (i % 3 == 0) {
        counts += 1
        sum += i
      }
    }
    printf("1...100的偶数中，是3的倍数的个数为%d个\n", counts)
    printf("这些数值的总和为%d\n", sum)

    // 打印99乘法口诀
    for (i <- 1 to 9) {
      for (j <- 1 to i) {
        print(j + "*" + i + "=" + (i*j) + "\t")
      }
      println()
    }

    // 将列表中的每个元素做平方除以5的操作，并将运算结果保存到变量res中
    val X = List(10,8,23,17,7)
    val res = for (i <- X) yield {
      (i * i)/5.0
    }
    println("res = " + res)

    // 计算1...100的和
    var i = 1
    var s = 0
    while (i <= 100) {
      s += i
      i += 1
    }
    println("1...100的总和为：" + s)

    // 用户登录邮箱，一共5次机会，如果输错则继续等待输入，并返回剩余登录次数
    breakable{
      var counts = 0
      while (true) {
        println("请输入用户名：")
        val user = StdIn.readLine()
        println("请输入密码：")
        val passwd = StdIn.readLine()

        if (user == "scala" && passwd == "123abc") {
          println("恭喜您，登录成功！")
          break()
        } else {
          if (counts < 4) {
            printf(s"输入的用户名或密码错误！您还剩${4 - counts}次输入机会！\n")
          } else {
            println("输入的用户名或密码错误！，请24小时之后再尝试！")
            break()
          }
        }
        counts += 1
      }
    }


  }
}
