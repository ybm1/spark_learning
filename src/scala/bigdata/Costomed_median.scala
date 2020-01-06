package scala.bigdata
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.bigdata.Customer_agg._
object Costomed_median {

    def main(args: Array[String]): Unit = {
      //System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master")

      val spark = SparkSession
        .builder
        .appName("InterfaceMonitor")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._
      val ds = Seq(
        ("20181102221610c07vy","10000011","10000032",20.0,1,20.0,0 ,"2019-04-19 22:16:10.0"),
        ("20181102221733dgvcv","10000011","10000032",20.0,1,20.0,0 ,"2019-04-19 22:17:34.0"),
        ("20181102222339oakpn","10000061","10000032",0.2 ,1,0.2 ,5 ,"2019-04-19 22:23:39.0"),
        ("20181102225503nhath","10000061","10000032",20.0,1,20.0,7 ,"2019-04-19 22:55:03.0"),
        ("201811030008236k9yy","10000061","10000032",0.2 ,1,0.2 ,5 ,"2019-04-19 00:08:23.0"),
        ("20181103005135do5zg","10000069","10000015",0.2 ,1,0.2 ,0 ,"2019-04-19 00:51:35.0"),
        ("20181103005148ptr7a","10000069","10000015",0.2 ,1,0.2 ,0 ,"2019-04-19 00:51:48.0"),
        ("20181103005148w9isk","10000069","10000015",0.2 ,1,0.2 ,5 ,"2019-04-19 00:51:48.0"),
        ("20181103005205b8gvm","10000069","10000015",0.2 ,1,0.2 ,0 ,"2019-04-19 00:52:05.0"),
        ("20181103015930m2cz0","10000011","10000063",30.0,1,30.0,0 ,"2019-04-19 01:59:30.0")
      ).toDF("order_id","play_uid","god_uid","price","num","order_amount","order_status","create_time")

      ds.groupBy($"god_uid")
        .agg(sum("num") as "total_sum",
          approx_count_distinct("order_id") as "order_num",
          mean("price"),
          percentile_approx($"price", lit(0.8)) as "Customed percentile")
        .show(10)
      spark.stop()

    }

}
