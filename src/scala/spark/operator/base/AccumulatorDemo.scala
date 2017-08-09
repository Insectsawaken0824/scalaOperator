package scala.spark.operator.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
  * Created by zhao on 2017/4/13.
  * 累加器:spark的一个全局变量
  */
object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("BroadcastDemo")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("src/name.txt",2)
    //创建一个累加器
    val accumulator: Accumulator[Int] = sc.accumulator(0)
    rdd.map(x=>{
      accumulator.add(1)
    }).collect()
    println(accumulator.value)
    sc.stop()
  }
}
