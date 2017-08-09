package scala.spark.sql.createdf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Created by zhao on 2017/6/9.
  */
object CreateDataFrameByProgrammatically {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() //创建sparkConf对象
    conf.setAppName("My First Spark App") //设置应用程序的名称，在程序运行的监控页面可以看到名称
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val people = sc.textFile("src/scores.txt")
    //如果schema中制定了除String以外别的类型   在构建rowRDD的时候要注意指定类型     例如： p(2).toInt
    val rowRDD: RDD[Row] = people.map(_.split("\t")).map(x=>{Row(x(0),x(1).toInt)})
    //通过StructType创建DataFrame的schema
    val schema: StructType = StructType.apply(Array(StructField("cla", StringType, true),StructField("score", IntegerType,true)))
    //  val arr = Array(StructField("name",StringType,true),StructField("age",IntegerType,true))
    //  val schema = StructType.apply(arr)
    //构造DataFrame
    val scroeDF: DataFrame = sqlContext.createDataFrame(rowRDD,schema)
    scroeDF.show()
    scroeDF.printSchema()
    /* peopleDataFrame.registerTempTable("clazzScore")
    val results = sqlContext.sql("SELECT score,clazz FROM clazzScore")
    //  results.map(t => "age: " + t(0)).collect().foreach(println)
    results.map(t => "clazz: " + t.getAs[String]("clazz")+"\tscore:"+t.getAs[Integer]("score")).foreach(println)*/
    sc.stop()
  }
}
