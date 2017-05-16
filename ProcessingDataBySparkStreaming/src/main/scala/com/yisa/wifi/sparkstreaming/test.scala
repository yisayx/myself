package com.yisa.wifi.sparkstreaming

import java.util.HashMap
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.broadcast.Broadcast
import com.yisa.wifi.manager.KafkaSink
import java.util.Properties
import org.apache.kafka.common.serialization.StringSerializer


object test {
  def main(args: Array[String]){
    var sparkConf = new SparkConf().setAppName("ProcessWifiData").setMaster("local")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.ui.port", "14047")
    
    var sc = new SparkContext(sparkConf)
    var ssc = new StreamingContext(sc, Seconds(3))
    
    //producer
    var kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerParams = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", "")//brokerId
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerParams))
    }
    
    
    println("*****************")
    var map = new HashMap[String, String]()
    map.put("aa", "labixiaoxin")
    map.put("aa", "xiaohongshu")
    map.put("aa", "taobao")
    map.put("bb", "11")
    map.put("bb", "22")
    map.put("bb", "33")
    map.put("bb", "44")
    map.put("bb", "55")
    
    println(map)
    
//    var arr: Array[(String, String)] = Array(map)
    
    var res0: Array[(String, Int)] = Array(("a",3), ("b",5))
    
    val a = sc.parallelize(List((1,2),(1,3),(3,4),(3,6)))
    a.reduceByKey((x,y) => x + y).collect
    
    val wordsCountWithReduce = a.
      reduceByKey(_+_).
      collect().
      foreach(println)
  }
}