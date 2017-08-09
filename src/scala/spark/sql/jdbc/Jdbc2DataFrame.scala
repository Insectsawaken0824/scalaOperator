package scala.spark.sql.jdbc

import java.util

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import java.util.HashMap

/**
  * Created by zhao on 2017/6/9.
  */
object Jdbc2DataFrame {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Jdbc2DataFrame").setMaster("local"))
    val sqlContext: SQLContext = new SQLContext(sc)
    val hashMap: util.HashMap[String, String] = new HashMap[String,String]()
    hashMap.put("url", "jdbc:mysql://node1:3306/testdb");
    hashMap.put("user", "spark");
    hashMap.put("password", "spark2016");
    //表
    hashMap.put("dbtable", "student_info");
    //读取数据创建DataFrame
    val studentInfosDF: DataFrame = sqlContext.read.format("jdbc").options(hashMap).load()
    //读另一张表
    hashMap.put("dbtable", "student_score");
    val studentScoresDF = sqlContext.read.format("jdbc").options(hashMap).load()

    studentInfosDF.registerTempTable("student_info")
    studentScoresDF.registerTempTable("student_score")
    val sql = "SELECT student_info.name,student_info.age,student_score.score"
      .+(" FROM student_info JOIN student_score ON (student_info.name = student_score.name)")
      .+(" WHERE student_score.score  > 80")
    sqlContext.sql(sql).show()
    sc.stop()
  }
}
