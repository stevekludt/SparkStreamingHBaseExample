package examples

import org.apache.spark.{SparkContext, SparkConf}
import it.nerdammer.spark.hbase._
/**
  * Created by stevekludt on 1/8/16.
  */
object HBaseRead {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseTest")
    val sc = new SparkContext(sparkConf)
    /**
    val sensorRDD = sc.hbaseTable[(String, Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double])]("sensor")
      .select("temp", "humidity", "psi", "co2", "flo", "chlPPM")
      .inColumnFamily("data")*/
    val sensorRDD = sc.hbaseTable[(String, Option[Double], Option[Double])]("sensor")
      .select("temp", "humiditybdiknvw")
      .inColumnFamily("data")

    sensorRDD.foreach(t => {
      if(t._2.nonEmpty) println(t)
    })
  }
}
