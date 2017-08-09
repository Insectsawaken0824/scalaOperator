package scala.spark.sql.createdf

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhao on 2017/6/9.
  * 通过json文件创建DataFrame
  */
object JsonCreateDF {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local").setAppName("JsonCreateDF"))
    val sqlContext: SQLContext = new SQLContext(sc)
    val peopleDF: DataFrame = sqlContext.read.json("src/people.json")
    peopleDF.write.mode(SaveMode.Overwrite).save("src/tempfile/parquetpeople")
    sc.stop()
  }
}
