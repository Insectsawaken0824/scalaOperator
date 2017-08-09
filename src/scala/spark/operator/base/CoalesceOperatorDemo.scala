package scala.spark.operator.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by zhao on 2017/4/13.
  */
object CoalesceOperatorDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("CoalesceOperatorDemo")
    val context: SparkContext = new SparkContext(conf)
    val list = Array(
      "a1",
      "a2",
      "a3",
      "a4",
      "a5",
      "a6",
      "a7",
      "a8",
      "a9",
      "a10",
      "a11",
      "a12"
    )
    val rdd: RDD[String] = context.makeRDD(list,6)
//    rdd.mapPartitionsWithIndex(lookPartitions).collect()

//    rdd.coalesce(3,false).mapPartitionsWithIndex(lookPartitions).collect()

    rdd.coalesce(12,true).mapPartitionsWithIndex(lookPartitions).collect()
  }

  def lookPartitions (index : Int,iterator : Iterator[String]) : Iterator[String] = {
    val list = new ListBuffer[String]
    while (iterator.hasNext){
      val str: String = iterator.next()
      println("Partitions:"+index+","+"result:"+str)
      list+=str
    }
    list.iterator
  }
}
