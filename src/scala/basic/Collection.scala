package scala.basic

object Collection {
  def main(args: Array[String]): Unit = {
    // 构造list时，可以显式指定类型
    val l1 : List[(Double,Double,Double)] = List((1,2,3.5),(2,1,3))
    val l2 : Seq[(Double,Double,Double)] = Seq((1,2,3.5),(2,1,3))

    l1.filter(_._3>2)
    l2.filter(_._3>2)
    List((1,2,3),(5,9.4,4)).filter(_._2.toString.toDouble>1)


    // 构造不可变列表
    val ls1 = List("apple","orange","banana","dog","cat","monkey","pig")
    val ls2 = List(10,22,16,4,8)

    // 构造可变列表
    import scala.collection.mutable
    var ls3 = mutable.ListBuffer(1,1,2,3,5,8,13)

    // 列表元素的增加
    ls3.append(21,36) // 增加多个元素
    println("ls3 = " + ls3)
    ls3.+=(57) // 增加一个元素
    println("ls3 = " + ls3)
    ls3.++=(ls2) // 将一个列表添加到可变列表中
    println("ls3 = " + ls3)
    ls3.insert(3,100) // 在指定位置插入元素
    println("ls3 = " + ls3)

    // 列表元素的删除
    ls3.trimStart(2) // 删除列表开头的两个元素
    println("ls3 = " + ls3)
    ls3.trimEnd(2) // 删除列表结尾的两个元素
    println("ls3 = " + ls3)
    ls3.-=(2,22) // 删除指定的值
    println("ls3 = " + ls3)
    // 列表元素的修改
      ls3(8) = 900 // 将第9个元素改为900
    println("ls3 = " + ls3)
    ls3.update(4,400) // 将第5个元素改为400
    println("ls3 = " + ls3)

    // 列表元素的查询
      println("列表ls1的第3个元素是：" + ls1(2))
    println("列表ls3元素的切片(第4个至第8个元素）：" + ls3.slice(3,8))
    println("根据条件筛选列表ls2的元素：" + ls2.filter(_>=10))

    // 列表元素的删除 -- 需要注意的是，如下方法均不可以之间改变列表本身
    println("删除列表前两个元素：" + ls2.drop(2))
    println("删除列表后两个元素：" + ls2.dropRight(2))
    // dropWhile：从左向右丢弃元素，直到条件不成立
    println("删除首批大于等于10的元素：" + ls2.dropWhile(_ >= 10))


    // 列表的属性
    println("列表ls1的元素个数为：" + ls1.length)
    println("列表ls3中偶数的个数：" + ls3.count(_ % 2 == 0))
    println("列表ls2元素的排序：" + ls2.sorted) // 如需降序，再使用reverse方法
    println("返回列表ls2中首次大于12的元素：" + ls2.find(_ > 12))
    // indexOf方法如果找不到元素的位置则返回-1
    println("返回ls2中元素值为12的首次位置：" + ls2.indexOf(12))

    /* 常规统计函数的属于
    ls2.length -- 计数
    ls2.sum -- 求和
    ls2.min -- 最小值
    ls2.max -- 最大值
    */

    // 列表ls3与ls2的差集
    println("差集为：" + ls3.diff(ls2))

    // 列表元素的拼接，结果为字符型对象
    println("列表元素的拼接：" + ls2.mkString(","))



    // 元组的构造--使用一对圆括号即可
    val t1 = (1,"First",5.5,"Monday",false,'A',100002345)
    // 元组元素的获取
    println("元组的第三个元素：" + t1._3)
    //相比于列表而言，元组可用的方法就少了很多，
    // 例如length方法、filter方法、drop方法、count方法等。
    // 如果你想使用列表的那些常规方法，也是可以的，
    // 那就对元组再使用productIterator方法，便可以基于此使用列表的其他方法了。

    println("删除开头的两个元素：" )
    t1.productIterator.drop(2).foreach(println)
    // 不可变映射
    val info = Map("name" -> "Snake", "gender" -> "男", "age" -> 30, ("height",170), "score" -> 88)

    // 可变映射
    var RFM = mutable.Map("Recency" -> 37, ("Frequency",8), "Money" -> 3274)
    // 元素的增加
    RFM.+=(("Uid" -> 213244532))
    println("RFM = " + RFM)

    // 元素的删除
    RFM.-=("Frequency")
    println("RFM = " + RFM)

    // 元素的修改
    RFM("Money") = 3330
    RFM.update("Recency", 307)
    println("RFM = " + RFM)
    // 映射元素的查询
    println("info中name键对应的值：" + info("name"))
    println("info中score键对应的值：" + info.get("score"))

    // 返回映射的键、值、键值对
    println("映射的键为：" + info.keySet)
    println("映射的值为：" + info.values)
    println("映射的键值对为：" + info.toList)

  }
}
