package scala.spark.operator.base

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by zhao on 2017/4/26.
  */
object DoubleReduceByKey {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("DoubleReduceByKey").setMaster("local"))
    sc.setLogLevel("WARN")
    var demoRDD: RDD[(String, Int)] = sc.parallelize(Array(("hello",1),("hello",1),("hello",1),("hello",1),("hello",1),("hello",1),("hello",1),("hello",1),("world",1),("spark",1)),4)
    val broadcast: Broadcast[String] = sc.broadcast(demoRDD.sample(true,0.3).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false).take(1)(0)._2)
    demoRDD = demoRDD.cache()
    demoRDD.filter(x => x._1!=broadcast.value).foreach(println)
    println("******************************************")
    demoRDD.mapPartitionsWithIndex(printPartitionsIndex,false).collect()
    println("------------------------------------------")
    demoRDD.groupByKey().mapPartitionsWithIndex((index,iterator)=>{
      val list = new ListBuffer[(String,Iterable[Int])]()
      while (iterator.hasNext) {
        val num = iterator.next()
        println("partitionId:" + index +"," + "value:" + num)
        list+=num
      }
      list.iterator
    },false).collect()
    println("------------------------------------------")
    var first: RDD[(String, Int)] = demoRDD.map(x => (new Random().nextInt(3) + "-" +x._1,x._2)).reduceByKey(_+_)
    first = first.cache()
    first.mapPartitionsWithIndex(printPartitionsIndex, false).collect()
    println("------------------------------------------")
    first.map(x=>(x._1.substring(2),x._2)).reduceByKey(_+_).mapPartitionsWithIndex(printPartitionsIndex,false).collect()
  }
  private def printPartitionsIndex(x:Int,y:Iterator[(String,Int)]) = {
    val list = new ListBuffer[(String,Int)]()
    while (y.hasNext) {
      val num = y.next()
      println("partitionId:" + x+"," + "value:" + num)
      list+=num
    }
    list.iterator
  }
}
