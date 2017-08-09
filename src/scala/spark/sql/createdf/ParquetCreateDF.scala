package scala.spark.sql.createdf

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhao on 2017/6/9.
  * 通过parquet格式文件创建DataFrame
  */
object ParquetCreateDF {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local").setAppName("ParquetCreateDF"))
    val sqlContext: SQLContext = new SQLContext(sc)
    val peopleDF: DataFrame = sqlContext.read.format("parquet").load("src/tempfile/parquetpeople")
    peopleDF.show()
    sc.stop()
  }
}
