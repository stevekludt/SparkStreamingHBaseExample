/*
 * 
 *  
 */

package examples

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import it.nerdammer.spark.hbase._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.eventhubs.EventHubsUtils

object HBaseSensorStream {
  final val tableName = "sensor"
  final val cfDataBytes = Bytes.toBytes("data")
  final val cfAlertBytes = Bytes.toBytes("alert")
  final val colHzBytes = Bytes.toBytes("hz")
  final val colDispBytes = Bytes.toBytes("disp")
  final val colFloBytes = Bytes.toBytes("flo")
  final val colSedBytes = Bytes.toBytes("sedPPM")
  final val colPsiBytes = Bytes.toBytes("psi")
  final val colChlBytes = Bytes.toBytes("chlPPM")

  val ehParams = Map[String, String](
    "eventhubs.policyname" -> "Spark",
    "eventhubs.policykey" -> "7rASPywwkir2ojPYZ1rwP3vt48j8p08QjZhIYoHPmZY=",
    "eventhubs.namespace" -> "datuheh-ns",
    "eventhubs.name" -> "datuh",
    "eventhubs.partition.count" -> "2",
    "eventhubs.consumergroup" -> "$default",
    "eventhubs.checkpoint.dir" -> "/home/stevekludt/stream/sparkchelpoint",
    "eventhubs.checkpoint.interval" -> "10"
  )

  // schema for sensor data   
  case class Sensor(resid: String, date: String, time: String, hz: Double, disp: Double, flo: Double, sedPPM: Double, psi: Double, chlPPM: Double)

  object Sensor {
    //function to return 0 if the input is not a double
    def toDouble(s: String): Double = {
      try {
        s.toDouble
      } catch {
        case e: Exception => 0
      }
    }

    // function to parse line of sensor data into Sensor class
    def parseSensor(str: String): Sensor = {
      val p = str.split(",")
      println(p)
      Sensor(p(0), p(1), p(2), toDouble(p(3)), toDouble(p(4)), toDouble(p(5)), toDouble(p(6)), toDouble(p(7)), toDouble(p(8)))
    }
    // my function to use spark to hbase connector
    def convertForHBase(sensor: Sensor): (String, Double, Double, Double, Double, Double, Double) = {
      val dateTime = sensor.date + "" + sensor.time
      val rowkey = sensor.resid + "_" + dateTime

      return (rowkey, sensor.hz, sensor.disp, sensor.flo, sensor.sedPPM, sensor.psi, sensor.chlPPM)
    }

    //  Convert a row of sensor object data to an HBase put object
    /**def convertToPut(sensor: Sensor): (ImmutableBytesWritable, Put) = {
      val dateTime = sensor.date + " " + sensor.time
      // create a composite row key: sensorid_date time
      val rowkey = sensor.resid + "_" + dateTime
      val put = new Put(Bytes.toBytes(rowkey))
      // add to column family data, column  data values to put object 
      put.add(cfDataBytes, colHzBytes, Bytes.toBytes(sensor.hz))
      put.add(cfDataBytes, colDispBytes, Bytes.toBytes(sensor.disp))
      put.add(cfDataBytes, colFloBytes, Bytes.toBytes(sensor.flo))
      put.add(cfDataBytes, colSedBytes, Bytes.toBytes(sensor.sedPPM))
      put.add(cfDataBytes, colPsiBytes, Bytes.toBytes(sensor.psi))
      put.add(cfDataBytes, colChlBytes, Bytes.toBytes(sensor.chlPPM))
      return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
    }*/
    // convert psi alert to an HBase put object
    def convertToPutAlert(sensor: Sensor): (ImmutableBytesWritable, Put) = {
      val dateTime = sensor.date + " " + sensor.time
      // create a composite row key: sensorid_date time
      val key = sensor.resid + "_" + dateTime
      val p = new Put(Bytes.toBytes(key))
      // add to column family alert, column psi data value to put object 
      p.add(cfAlertBytes, colPsiBytes, Bytes.toBytes(sensor.psi))
      return (new ImmutableBytesWritable(Bytes.toBytes(key)), p)
    }
  }

  def main(args: Array[String]): Unit = {
    // set up HBase Table configuration
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/home/stevekludt/out")
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val sparkConf = new SparkConf().setAppName("HBaseStream")
    // create a StreamingContext, the main entry point for all streaming functionality
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    // parse the lines of data into sensor objects
    //val sensorDStream = ssc.textFileStream("/home/stevekludt/stream").map(Sensor.parseSensor)
    //sensorDStream.print()
    val sensorDStream = EventHubsUtils.createUnionStream(ssc, ehParams).map(s => Bytes.toString(s)).map(Sensor.parseSensor)
    sensorDStream.print()

    sensorDStream.foreachRDD { rdd =>
      // filter sensor data for low psi
      val alertRDD = rdd.filter(sensor => sensor.psi < 5.0)
      alertRDD.take(1).foreach(println)
      // convert sensor data to put object and write to HBase table column family data
      /**rdd.map(Sensor.convertToPut).
        saveAsHadoopDataset(jobConfig)*/
      rdd.map(Sensor.convertForHBase).
        toHBaseTable(tableName)
        .toColumns("hz", "disp", "flo", "sedPPM", "psi", "chlPPM")
        .inColumnFamily("data")
        .save()

      rdd.map(Sensor.convertForHBase).take(100).foreach(println)
      // convert alert data to put object and write to HBase table column family alert
      /**alertRDD.map(Sensor.convertToPutAlert).
        saveAsHadoopDataset(jobConfig)*/
    }
    // Start the computation
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()

  }

}