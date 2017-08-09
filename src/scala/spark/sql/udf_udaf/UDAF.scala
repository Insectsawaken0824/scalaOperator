package scala.spark.sql.udf_udaf

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by zhao on 2017/6/10.
  */
object UDAF {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("UDF").setMaster("local"))
    val sqlContext: SQLContext = new SQLContext(sc)

    val name: Array[String] = Array("yarn", "Marry", "Jack", "Tom")
    val namesRDD: RDD[String] = sc.parallelize(name, 4)
    val namesRowRDD: RDD[Row] = namesRDD.map(name => Row(name))

    val structType: StructType = StructType.apply(Array(StructField("name", StringType, true)))
    val nameDF: DataFrame = sqlContext.createDataFrame(namesRowRDD, structType)
    nameDF.registerTempTable("names")

    sqlContext.udf.register("strCount", new StringCount)

    // 使用自定义函数
    sqlContext.sql("select name,strCount(name) from names group by name")
      .collect()
      .foreach(println)
  }
}
