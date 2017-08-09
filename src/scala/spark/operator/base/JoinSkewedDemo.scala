package scala.spark.operator.base

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by zhao on 2017/4/26.
  */
object JoinSkewedDemo {
  def main(args: Array[String]): Unit = {
    val random : Int = 3
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("DoubleReduceByKey").setMaster("local"))
    sc.setLogLevel("WARN")
    var demoRDD1: RDD[(String, Int)] = sc.parallelize(Array(("hello",1),("hello",2),("hello",3),("hello",4),("hello",5),("hello",6),("hello",7),("hello",8),("world",1),("spark",1)))
    demoRDD1 = demoRDD1.cache()
    var demoRDD2: RDD[(String, Int)] = sc.parallelize(Array(("hello",111),("world",222),("spark",333)))
    demoRDD2 = demoRDD2.cache()
    val broadcast: Broadcast[String] = sc.broadcast(demoRDD1.sample(true,0.3).map(x => {(x._1,1)}).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false).take(1)(0)._2)
    val randomPrefixRDD: RDD[(String, Int)] = demoRDD1.filter(x => x._1==broadcast.value).map(x => (new Random().nextInt(random) + "-" +x._1,x._2))
    val demoRDD2x2: RDD[(String, Int)] = demoRDD2.filter(x => x._1==broadcast.value).flatMap(x => {
      var flag: Int = random - 1
      val list: ListBuffer[(String, Int)] = new ListBuffer[(String, Int)]
      while (flag >= 0) {
        val tuple: (String, Int) = (flag + "-" + x._1, x._2)
        list += tuple
        flag -= 1
      }
      list
    })
    val result1: RDD[(String, (Int, Int))] = randomPrefixRDD.join(demoRDD2x2).map(x=>(x._1.substring(2),x._2))
    val commonRDD1: RDD[(String, Int)] = demoRDD1.filter(x => x._1!=broadcast.value)
    val commonRDD2: RDD[(String, Int)] = demoRDD2.filter(x => x._1!=broadcast.value)
    commonRDD1.join(commonRDD2).union(result1).foreach(println)
  }
}
