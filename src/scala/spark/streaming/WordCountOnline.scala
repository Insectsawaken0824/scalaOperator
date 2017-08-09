package scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by zhao on 2017/6/11.
  * 1、local的模拟线程数必须大于等于2 因为一条线程被receiver(接受数据的线程)占用，另外一个线程是job执行
  * 2、Durations时间的设置，就是我们能接受的延迟度，这个我们需要根据集群的资源情况以及监控，ganglia  每一个job的执行时间
  * 3、 创建StreamingContext有两种方式 （sparkconf、sparkcontext）
  * 4、业务逻辑完成后，需要有一个output operator
  * 5、JavaStreamingContext.start()
  * 6、JavaStreamingContext.stop()无参的stop方法会将sparkContext一同关闭，stop(false)
  * 7、JavaStreamingContext.stop() 停止之后是不能在调用start
  * 8、JavaStreamingContext.start() straming框架启动之后是不能在次添加业务逻辑
  */
object WordCountOnline {
  def main(args: Array[String]): Unit = {
    //local的线程数必须大于等于2,因为线程被receiver(接受数据的线程)占用，另外一个线程是job执行
    val conf: SparkConf = new SparkConf().setAppName("WordCountOnline").setMaster("local[2]")
    //创建StreamingContext,时间间隔必须大于job执行时间,并且在可接受范围内
    val ssc: StreamingContext = new StreamingContext(conf,Durations.seconds(5))
    //设置监视端口,并设置持久化级别
    val linesDStream: ReceiverInputDStream[String] = ssc.socketTextStream("node1",9999,StorageLevel.MEMORY_AND_DISK)
    val wordDStream: DStream[String] = linesDStream.flatMap(_.split(" "))
    val resultDStream: DStream[(String, Int)] = wordDStream.map((_,1)).reduceByKey(_+_)
    //业务逻辑完成后  需要有一个output operator执行
    resultDStream.print()
    //启动后无法再次添加业务逻辑  并且关闭后无法再次启动.
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
