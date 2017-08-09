package scala.spark.operator.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhao on 2017/4/12.
  * distinct利用groupByKey去重
  */
object DistinctOperatorDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("DistinctOperatorDemo")
    val context: SparkContext = new SparkContext(conf)
    //makeRDD将数组转成RDD元素
    val arRDD: RDD[(String, Int)] = context.makeRDD(Array(("11", 1),("22", 2),("22", 2),("33", 3),("44", 4),("55", 5)))
    arRDD.distinct().foreach(print)
    println()

    //将原数据作为新元祖的key,后边可用groupByKey去重
    val map: RDD[((String, Int), Int)] = arRDD.map((_,1))
    map.groupByKey().foreach(x=>{
      println(x._1)
    })
    context.stop()
  }
}
