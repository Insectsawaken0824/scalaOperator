package scala.spark.operator.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by zhao on 2017/4/12.
  */
object MapOperatorDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("MapOperatorDemo")
    val context: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = context.makeRDD(1 to 10,3)
    val map: RDD[String] = rdd.map(_+",")
    /**
      * 想要执行，必须有action类的算子
      * collect算子会将集群中计算的结果回收到Driver端，慎用
      */
    map.collect().foreach(println)
    map.foreach(println)
    rdd.filter(_!=2).foreach(println)

    /**
      * mapPartitions这个算子遍历的单位是partition
      * 	会将一个partition的数据量全部加载到一个集合里面
      */
    val partitions: RDD[Int] = rdd.mapPartitions(iterator => {
      val list = new ListBuffer[Int]()
      while (iterator.hasNext) {
        val next: Int = iterator.next()
        list += next
      }
      list.iterator
    }, false)
    partitions.foreach(println)


    rdd.mapPartitionsWithIndex((index,iterator)=>{
      val list = new ListBuffer[Int]()
      while (iterator.hasNext) {
        val num = iterator.next()
        println("partitionId:" + index +"," + "value:" + num)
        list+=num
      }
      list.iterator
    }, false).collect()

    /**
      * 释放资源
      */
    context.stop()
  }
}
