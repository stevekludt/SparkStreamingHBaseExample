/*
 * 
 *  
 */

package com.datuh.sparkStream

import java.util.{Date, Properties}

import org.apache.hadoop.hbase.util.Bytes
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.SparkConf
import it.nerdammer.spark.hbase._
import com.datuh.Utils.DateTimeUtils
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.eventhubs.EventHubsUtils
import com.datuh.ML.StreamLinearRegression
import com.github.nscala_time.time.Imports._

object SensorStream {
  final val tableName = "sensor"

  val ehParams = Map[String, String](
    "eventhubs.policyname" -> "Spark",
    "eventhubs.policykey" -> "7rASPywwkir2ojPYZ1rwP3vt48j8p08QjZhIYoHPmZY=",
    "eventhubs.namespace" -> "datuheh-ns",
    "eventhubs.name" -> "datuh",
    "eventhubs.partition.count" -> "2",
    "eventhubs.consumergroup" -> "$default",
    "eventhubs.checkpoint.dir" -> "/home/stevekludt/stream/sparkcheckpoint",
    "eventhubs.checkpoint.interval" -> "10"
  )

  // schema for sensor data
  case class Sensor(deviceID: String, date: String, time: String, temp: Double, humid: Double, flo: Double, co2: Double, psi: Double, chlPPM: Double)

  object Sensor {
    //function to return 0 if the input is not a double
    def toDouble(s: String): Double = {
      try { s.toDouble }
      catch { case e: Exception => 0 }
    }

    // function to parse line of sensor data into Sensor class
    def parseSensor(str: String): Sensor = {
      val p = str.split(",")
      Sensor(p(0), p(1), p(2), toDouble(p(3)), toDouble(p(4)), toDouble(p(5)), toDouble(p(6)), toDouble(p(7)), toDouble(p(8)))
    }
    // my function to use spark to hbase connector
    def convertForHBase(sensor: Sensor): (String, Double, Double, Double, Double, Double, Double) = {
      val dateTime = sensor.date + "T" + sensor.time
      val rowkey = sensor.deviceID + "_" + dateTime

      return (rowkey, sensor.temp, sensor.humid, sensor.flo, sensor.co2, sensor.psi, sensor.chlPPM)
    }

  }

  def createContext() : StreamingContext = {
    val driverPort = 7777
    val sparkConf = new SparkConf()
      .setAppName("DatuhStreaming")
      .set("spark.logConf", "true")
      .set("spark.driver.port", driverPort.toString)
      .set("spark.akka.logLifecycleEvents", "true")
      //.set("spark.cassandra.connection.host", "localhost")

    // create a StreamingContext, the main entry point for all streaming functionality
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // parse the lines of data into sensor objects
    val sensorDStream = EventHubsUtils.createUnionStream(ssc, ehParams)
      .map(s => Bytes.toString(s))
      .map(Sensor.parseSensor)

    val windowedStream = sensorDStream.window(Minutes(1))

    // Perform Streaming Linear Regression
    val mlStream = sensorDStream.map(row => (row.temp, DateTimeUtils.toDate(row.date, row.time), DateTimeUtils.getDateforML(row.date, row.time)))
    //perform Stream Linear Regression on the DStream
    StreamLinearRegression.streamPredict(ssc, mlStream)


    //this next line is for testing
    mlStream.print()

    //write to casandra where keyspace = Sensor and table = events
    //TODO: Get the Write to Cassandra Working
    //was running into class compatibility issues with the google guava utility.  It's already included in hadoop
    //and is needed by cassandra, but they are on different versions.  Could not get this resolved so going with HBASE
    //sensorDStream.saveToCassandra(tableName, "events", SomeColumns("deviceID", "date", "time", "temp", "humid", "flo", "co2", "psi", "chlPPM"))


    sensorDStream.foreachRDD { rdd =>
      // filter sensor data for low psi
      //val alertRDD = rdd.filter(sensor => sensor.psi < 5.0)

      // convert sensor data to for use by nerdammer and write to HBase table column family data
      rdd.map(Sensor.convertForHBase)
        .toHBaseTable(tableName)
        .toColumns("temp", "humidity", "flo", "co2", "psi", "chlPPM")
        .inColumnFamily("data")
        .save()

      //Send to Kafka
          //TODO: need to use a producer pool for kafka
          //example can be found at: www.michael-noll.com/blog/2014/10/01/kafka-spark-streaming-integration-example-tutorial
      rdd.foreachPartition(partitionOfRecords => {
        val topic = "spark"
        val brokers = "localhost:9092"
        val props = new Properties()
        props.put("metadata.broker.list", brokers)
        props.put("serializer.class", "kafka.serializer.StringEncoder")
        props.put("producer.type", "async")
        val config = new ProducerConfig(props)
        val producer = new Producer[String, String](config)

        partitionOfRecords.foreach { e =>
          //should try to convert the message to an json objec here before sending
          val event = new KeyedMessage[String, String](topic, e.deviceID, e.toString)
          producer.send(event)
        }
        producer.close()
      })
    }
    ssc
  }

  def main(args: Array[String]): Unit = {

    // Get StreamingContext from checkpoint directory or create a new one
    val ssc = StreamingContext.getOrCreate(ehParams("eventhubs.checkpoint.dir"),
      () => {
        createContext()
      })

    // Start the computation
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()
  }
}