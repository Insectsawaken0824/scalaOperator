package scala.spark.sql.hive

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhao on 2017/6/10.
  * 不能在本地测试
  * 只能提交到集群上
  */
object SparkOnHive {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("SparkOnHive"))
    val hiveContext: HiveContext = new HiveContext(sc)
    hiveContext.sql("DROP TABLE IF EXISTS student_infos")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos(name STRING, age INT) row format delimited fields terminated by '\t'")
    hiveContext.sql("LOAD DATA LOCAL INPATH '/root/resource/student_infos' INTO TABLE student_infos")

    hiveContext.sql("DROP TABLE IF EXISTS student_scores")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT) row format delimited fields terminated by '\t'")
    hiveContext.sql("LOAD DATA LOCAL INPATH '/root/resource/student_scores' INTO TABLE student_scores")

    val goodStudentsDF: DataFrame = hiveContext.sql("SELECT si.name, si.age, ss.score" +
      "FROM student_infos si" +
      "JOIN student_scores ss ON si.name=ss.name" +
      "WHERE ss.score>=80")

    hiveContext.sql("DROP TABLE IF EXISTS good_student_infos")
//    goodStudentsDF.saveAsTable("good_student_infos")
    goodStudentsDF.write.saveAsTable("good_student_infos")

    val goodStudentRows: Array[Row] = hiveContext.table("good_student_infos").collect()
    goodStudentRows.foreach(println)
    sc.stop()
  }
}
