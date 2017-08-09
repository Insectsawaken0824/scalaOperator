package scala.spark.sql.udf_udaf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.Int
import scala.util.Random

/**
  * Created by zhao on 2017/6/10.
  */
object UDF {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("UDF").setMaster("local"))
    val sqlContext: SQLContext = new SQLContext(sc)

    val name: Array[String] = Array("yarn", "Marry", "Jack", "Tom")
    val namesRDD: RDD[String] = sc.parallelize(name, 4)
    val namesRowRDD: RDD[Row] = namesRDD.map(name => Row(name))

    val structType: StructType = StructType.apply(Array(StructField("name", StringType, true)))
    val nameDF: DataFrame = sqlContext.createDataFrame(namesRowRDD, structType)
    nameDF.registerTempTable("names")
    //一个参数
//    sqlContext.udf.register("strLen",(str: String) => str.length())
//    sqlContext.sql("select name,strLen(name) len from names").show()
    //多个参数
    sqlContext.udf.register("strLen",(str: String,random:Int) => str.length()+Random.nextInt(random))
    sqlContext.sql("select name,strLen(name,100) len from names").show()
    sc.stop()
  }
}
