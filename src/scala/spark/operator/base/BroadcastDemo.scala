package scala.spark.operator.base

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhao on 2017/4/13.
  * 广播变量
  *   只能在Driver端修改  不能再Executor端修改
  */
object BroadcastDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("BroadcastDemo")
    val sc = new SparkContext(conf)
    /**
      * filterDemoname.txt
test.txt
      */
    //    val rdd: RDD[Int] = sc.makeRDD(1 to 10,3)
    //    //广播变量
    //    val broadcast: Broadcast[Int] = sc.broadcast(6)
    //    rdd.filter(x=>{
    //      x != broadcast.value
    //    }).foreach(println)

    /**
      * broadlistDemo
      */
    val rdd: RDD[String] = sc.textFile("src/name.txt")
    val flatMapRDD: RDD[String] = rdd.flatMap(_.trim().split(" "))
    val broadcast: Broadcast[List[String]] = sc.broadcast(List("陈霞玻","陈欢兆","陈政竹"))
    val filter: RDD[String] = flatMapRDD.filter(x => {
      val list: List[String] = broadcast.value
      val it: Iterator[String] = list.iterator
      !list.contains(x) || ""==x.trim
    })
    filter.foreach(println)
    sc.stop()
  }
}
