package scala.spark.sql.createdf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhao on 2017/6/9.
  * 通过JSON格式的RDD创建DataFrame
  */
object JsonRddCreateDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() //创建sparkConf对象
    conf.setAppName("My First Spark App") //设置应用程序的名称，在程序运行的监控页面可以看到名称
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val infos = Array("{'name':'zhangsan', 'age':55}","{'name':'lisi', 'age':30}","{'name':'wangwu', 'age':19}")
    val scores = Array("{'name':'zhangsan', 'score':155}","{'name':'lisi', 'score':130}")

    val infoRdd = sc.parallelize(infos)
    val scoreRdd = sc.parallelize(scores)

    val infoDF = sqlContext.read.json(infoRdd)
    val scoreDF = sqlContext.read.json(scoreRdd)

    //    infoDF.registerTempTable("people")

    //    sqlContext.sql("select * from people where age > 20").show()
    infoDF.join(scoreDF, infoDF("name").===(scoreDF("name"))).select(infoDF("name"),infoDF("age"),scoreDF("score")).show()

    /* df.show()

     df.printSchema()

     df.select("name").show()

     df.select(df("name"), df("age")+10).show()

     df.filter(df("age")>10).show()

     df.groupBy("age").count.show()*/
    sc.stop()
  }
}
