package scala.spark.operator.base

import org.apache.spark.rdd.{RDD, SequenceFileRDDFunctions}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.{IntWritable, Text}

/**
  * Created by zhao on 2017/4/11.
  */
object SparkScalaDemo {
  def main(args: Array[String]): Unit = {
    val spconf = new SparkConf().setAppName("wordcount").setMaster("local")
    val sparkContext = new SparkContext(spconf)
    val file: RDD[String] = sparkContext.textFile("src/test.txt")
    val words: RDD[String] = file.flatMap(_.split(" "))
    val kv: RDD[(String, Int)] = words.map((_,1))
    val rs: RDD[(String, Int)] = kv.reduceByKey(_+_)
    val ra = rs.map(x=>{
      val text: Text = new Text(x._1.getBytes())
      val writable: IntWritable = new IntWritable(x._2)
      (text,writable)
    })
    ra.count()
    new SequenceFileRDDFunctions(ra).saveAsSequenceFile("src/seqtest.txt")
    /**
      * 释放资源
      */
    sparkContext.stop()
  }
}
