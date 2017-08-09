package scala.spark.sql.windowfun

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhao on 2017/6/11.
  * 如果SQL语句里面使用到了开窗函数，那么这个SQL语句必须使用HiveContext来执行，HiveContext默认情况下在本地无法创建
  */
object WindowFunction {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("WindowFunction"))
    val hiveContext: HiveContext = new HiveContext(sc)

    // 通过hiveContext操作hive数据库 删除已经存在的表，创建新表，并且加载数据
    hiveContext.sql("DROP TABLE IF EXISTS sales")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS sales(product STRING,category STRING,revenue STRING) " +
      "row format delimited fields terminated by '\t'")
    hiveContext.sql("LOAD DATA LOCAL INPATH '/root/resource/sales.txt' INTO TABLE sales")

    /**
      * row_number()开窗函数的作用：按照我们每一个分组的数据，按其照顺序，打上一个分组内的行号
      * id=2016 [111,112,113]
      * 那么对这个分组的每一行使用row_number()开窗函数后，三行数据会一次得到一个组内的行号
      * id=2016 [111 1,112 2,113 3]
      */
    val top3SalesDF: DataFrame = hiveContext.sql("SELECT product,category,revenue FROM " +
      "(SELECT product,category,revenue,row_number() OVER (PARTITION BY category ORDER BY revenue DESC) rank FROM sales) tmp_sales WHERE rank <= 3")
    hiveContext.sql("use result")
    hiveContext.sql("DROP TABLE IF EXISTS top3sales")
    top3SalesDF.write.saveAsTable("top3sales")
    sc.stop()
  }
}
