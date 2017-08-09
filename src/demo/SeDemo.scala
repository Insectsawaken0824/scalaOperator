package demo

import org.apache.hadoop.hive.serde2.io.ByteWritable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.{IntWritable, LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.spark.rdd.RDD

/**
  * Created by zhao on 2017/7/6.
  */
object SeDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SeDemo")
    conf.set("spark.sql.shuffle.partitions", "10")
      .set("spark.default.parallelism", "100")
      .set("spark.storage.memoryFraction", "0.5")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.shuffle.file.buffer", "64")
      .set("spark.shuffle.memoryFraction", "0.3")
      .set("spark.reducer.maxSizeInFlight", "24")
      .set("spark.shuffle.io.maxRetries", "60")
      .set("spark.shuffle.io.retryWait", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[NerDomain])) //使用Kryo序列化类库
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val file= sc.sequenceFile("src/seqtest.txt",classOf[Text],classOf[IntWritable])
    file.map(x=>{
      println(new String(x._1.getBytes))
      println(x._2.get())
      println("-----------")
    }).count()
  }
}
