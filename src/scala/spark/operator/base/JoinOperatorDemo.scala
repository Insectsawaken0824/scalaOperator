package scala.spark.operator.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhao on 2017/4/12.
  *
  */
object JoinOperatorDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("JoinOperatorDemo")
    val context: SparkContext = new SparkContext(conf)
    //设置日志级别
    context.setLogLevel("WARN")

    val rdd1: RDD[(String, Int)] = context.makeRDD(Array(("11", 1),("22", 2),("22", 2),("33", 3),("44", 4),("55", 5)))
    val rdd2: RDD[(String, Int)] = context.makeRDD(Array(("11", 1),("2", 221),("22", 223),("3", 333),("44", 444),("55", 567)))

    //内连接
//    val join: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
//    join.foreach(connectInt)

    //左外连接
//    val leftOuterJoin: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
//    leftOuterJoin.foreach(connectIntOptionLeft)

    //右外链接
//    val rightOuterJoin: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)
//    rightOuterJoin.foreach(connectIntOptionRight)

    //全连接
    val fullOuterJoin: RDD[(String, (Option[Int], Option[Int]))] = rdd1.fullOuterJoin(rdd2)
    fullOuterJoin.foreach(connectIntOptionFull)
    /**
      * 释放资源
      */
    context.stop()
  }

  def connectInt(a: Tuple2[String,Tuple2[Int,Int]]): Unit = {
    println(a._1+":"+a._2._1+","+a._2._2)
  }

  //Option  http://www.yiibai.com/scala/scala_options.html
  def connectIntOptionLeft(a: (String, (Int, Option[Int]))): Unit = {
    val iterator: Iterator[Int] = a._2._2.iterator
    var x = "";
    while(iterator.hasNext){
      x=x+iterator.next()+",";
    }
    println(a._1+":"+a._2._1+","+x)
  }
  def connectIntOptionRight(a: (String, (Option[Int],Int))): Unit = {
    val iterator: Iterator[Int] = a._2._1.iterator
    var x = "";
    while(iterator.hasNext){
      x=x+iterator.next()+",";
    }
    println(a._1+":"+a._2._2+","+x)
  }
  def connectIntOptionFull(a: (String, (Option[Int],Option[Int]))): Unit = {
    val iterator1: Iterator[Int] = a._2._1.iterator
    val iterator2: Iterator[Int] = a._2._2.iterator
    var x = "";
    while(iterator1.hasNext){
      x=x+iterator1.next()+",";
    }
    while(iterator2.hasNext){
      x=x+iterator2.next()+",";
    }
    println(a._1+":"+","+x)
  }
}
