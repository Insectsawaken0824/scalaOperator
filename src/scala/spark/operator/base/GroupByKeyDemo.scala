package scala.spark.operator.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhao on 2017/4/12.
  */
object GroupByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("GroupByKeyDemo")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = context.makeRDD(Array(("11", 1),("22", 2),("22", 2),("33", 3),("44", 4),("55", 5)))
    rdd.groupByKey().foreach(x=>{
      print(x._1+":")
      for (a<-x._2){
        println(a)
      }
    })
    rdd.reduceByKey(_+_).foreach(println)
    context.stop()
  }
}
