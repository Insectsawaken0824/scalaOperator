package scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by zhao on 2017/6/11.
  * UpdateStateByKey的主要功能:
  * 1、Spark Streaming中为每一个Key维护一份state状态，state类型可以是任意类型的的， 可以是一个自定义的对象，那么更新函数也可以是自定义的。
  * 2、通过更新函数对该key的状态不断更新，对于每个新的batch而言，Spark Streaming会在使用updateStateByKey的时候为已经存在的key进行state的状态更新
  * 	（对于每个新出现的key，会同样的执行state的更新函数操作），
  *
  * hello,3
  * bjsxt,2
  *
  * 如果要不断的更新每个key的state，就一定涉及到了状态的保存和容错，这个时候就需要开启checkpoint机制和功能
  *
  * 全面的广告点击分析
 *
  * @author zfg
  *
  * 有何用？   统计广告点击流量，统计这一天的车流量，统计。。。。点击量
  */
object UpdateStateByKeyOperator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKey")
    val ssc: StreamingContext = new StreamingContext(conf,Durations.seconds(5))

    /**
      * 多久会将内存中的数据（每一个key所对应的状态）写入到磁盘上一份呢？
      * 如果你的batch interval小于10s  那么10s会将内存中的数据写入到磁盘一份
      * 如果bacth interval 大于10s，那么就以bacth interval为准
      */
    ssc.checkpoint("hdfs://node1:9000/ssccheckpoint")
    val rid: ReceiverInputDStream[String] = ssc.socketTextStream("node1",9999)
    val word: DStream[(String, Int)] = rid.flatMap(_.split(" ")).map((_,1))
    val result: DStream[(String, Int)] = word.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      //创建一个变量，用于记录单词出现次数
      var newValue = state.getOrElse(0) //getOrElse相当于if....else.....
      for (value <- values) {
        newValue += value //将单词出现次数累计相加
      }
      Option(newValue)
    })
    result.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
