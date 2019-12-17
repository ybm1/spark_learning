package scala.basic
import scala.collection.mutable.ArrayBuffer // 构造变长数组
import scala.Array._ //构造二维数组
// 基本数据结构：数组
object Array_basic {
  def main(args: Array[String]): Unit = {
// 创建数组
    val arr1 = Array(1,2,1)
    val arr2 = new Array[String](3) // 如果用new来创建数组，必须指定长度
    arr2(0) = "aa"
    arr2(1) = "b"
    arr2(2) = "cc"
    println("arr1 = "+arr1.mkString(","))
    println("arr2 = "+arr2.mkString(","))
    var arr3 = "A"
    arr3 = "B"
    println("arr3 = "+arr3.mkString(","))

// 构造变长数组

    val arr4 = Array(1,10,100,1000)
    // 定义可变数组
    val arr5 = ArrayBuffer[Double]()
    for (i <- arr4) {
      arr5 += math.log(i) // 类似于Python中列表的append方法
    }
    println("arr5 = " + arr5.mkString(",")+ "\n" + arr5(0))
    //二维数组的本质就是一个矩阵，由行和列构成，
    // 该类数组的构造需要使用ofDim函数，同样ofDim函数的使用也需要导入模块，
    // 即scala.Array模块。
    // 构造二维数组
    val arr2_1 = ofDim[Int](3, 4)
    // 往数组中写值
    arr2_1(0)(0) = 1  ;  arr2_1(0)(1) = 10
    arr2_1(0)(2) = 100  ;  arr2_1(0)(3) = 100
    for (i <- 1 until 3) {
      for (j <- 0 until 4){
        arr2_1(i)(j) = (i + j) * 100
      }
    }

    // 打印二维数组
    println("arr2_1 =",arr2_1.toList )
    for (i <- 0 until 3) {
      for (j <- 0 until 4) {
        println(arr2_1(i)(j) + " ")
      }
      println()
    }
// 数组操作
   //数组元素的增操作，只能针对变长数组，可以使用很多种方法，
    // 例如+=法，++=法，append法以及insert法。

var a1 = ArrayBuffer[String]()
    a1 += "one"
    a1 ++= Array("a","b","c")
    a1.append("lll")
    a1.append("kk","jj")
    a1.insert(0,"111")
    a1.insert(2,"222")
println("a1 = ",a1)

// 数组元素的删操作，同样也只能针对变长数组，可以使用很多种方法，
    // 例如remove法、trimStart法和trimEnd法。
    a1.remove(0) // 删除一个元素
    a1.remove(3,2) // 从第3个元素开始，删除2个元素
    a1.trimEnd(2) // 删除末尾的两个元素
    a1.trimStart(2) // 删除开头的两个元素
    println("a1 = ",a1)
    //数组元素的改操作，既可以适用于定长数组，也可以适用于变长数组。
    // 修改元素的思想就是取而代之，即先通过索引的方式指定被修改的值，然后利用赋值的方式完成值的修改。
    a1(0) = "fuck"
    println("a1 = ",a1)

    //可以利用数组的distinct方法实现元素的排重，但需要注意的是，
    // 排重操作并不能对数组产生变动，即不影响原始数组。
    val A3 = Array(1,2,10,1,20,2,1,3,2,20)
    val A3_Dupli = A3.distinct  // 必须将排重的结果重新赋值给新的变量
    println("A3排重后的结果为：" + A3_Dupli.mkString(","))
    //利用数组的sorted方法完成元素的排序，该方法默认为升序排序，
    // 如需降序还需要再使用reverse方法（即翻转）。同样，该方法无法改变原始数组。
    val A3_Sort = A3.sorted // 默认对数组A3作升序排序
    println("A3升序后的结果为：" + A3_Sort.mkString(","))


    val A3_Sort_reverse = A3.sorted.reverse // 对数组A3作降序排序
    println("A3降序后的结果为：" + A3_Sort_reverse.mkString(","))
   // 当数组的元素为数值类型时，我们可以对数组作一些基本的统计元素，如计数、求和、最小值、最大值等。
    println("数组A3元素的总和为" + A3.sum)
    println("数组A3元素的最小值为" + A3.min)
    println("数组A3元素的最大值为" + A3.max)
    printf("数组A3中有%d个元素大于2\n", A3.count(x => x >2))
// 除了可以对数组作统计运算，还可以做元素级别的运算，
    // 只需要借助foreach方法或map方法就可以轻松实现。
    // 需要注意的是，foreach方法没有返回值，但map方法是有返回值的。
    println("A3每个数除于2.0的结果为：")
    A3.foreach(x => println(" " + x/2.0))
    println()
    println("A3每个数除于4.0的结果为：",A3.map(_/4.0).mkString(","))
// 筛选操作
    println("A3数组中所有大于2的元素为：",A3.filter(x => x >2).toList)



  }
}
