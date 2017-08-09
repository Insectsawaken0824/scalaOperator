package scala.spark.operator.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhao on 2017/4/12.
  * map和flatmap比较
  */
object FlatMapOperatorDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("FlatMapOperatorDemo")
    val context: SparkContext = new SparkContext(conf)
    val rdd: RDD[String] = context.parallelize(Array("hello,world","hello,spark"),1)
    rdd.map(_.split(",")).foreach(println)
    rdd.flatMap(_.split(",")).foreach(println)
    context.stop()
  }
}
