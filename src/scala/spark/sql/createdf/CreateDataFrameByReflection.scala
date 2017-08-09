package scala.spark.sql.createdf

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Created by zhao on 2017/6/9.
  * 通过自定义对象反射创建DataFrame
  */
object CreateDataFrameByReflection {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() //创建sparkConf对象
    conf.setAppName("My First Spark App") //设置应用程序的名称，在程序运行的监控页面可以看到名称
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val peopleRDD: RDD[String] = sc.textFile("src/Peoples.txt")
    val personRDD: RDD[Person] = peopleRDD.map(x => {
      val split: Array[String] = x.split(",")
      Person(split(1), split(2).trim().toInt)
    })
    //不引入包的话 toDF方法找不到
    import sqlContext.implicits._
    val personDf: DataFrame = personRDD.toDF()
    personDf.show()
    //注册成一张临时表
    personDf.registerTempTable("people")
    //执行sql语句
    val selectDF: DataFrame = sqlContext.sql("select name,age from people")
    //对DataFrame执行map算子后返回的是Row对象的RDD
    selectDF.map(t => "Name: " + t(0)).foreach(println)//根据位置取
    selectDF.map(t => "Name: " + t.getAs[String]("name")).foreach(println)//根据字段取
    selectDF.show()
    sc.stop()
  }
}
