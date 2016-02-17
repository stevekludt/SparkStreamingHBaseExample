package sparkStream

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import it.nerdammer.spark.hbase._
import org.apache.spark.sql.functions.explode

/**
  * Created by stevekludt on 1/8/16.
  */
object HBaseRead {
  def getJSONConfig(sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._

    val path = "/home/stevekludt/IdeaProjects/SparkStreamingHBaseExample/data/customerSensors.json"
    val config = sqlContext.read.json(path)
    config.registerTempTable("sensorConfig")

    //The following lines put the "Fields" values into an array with the SensorName
    val columns = Seq($"sensors.sensorName", $"sensors.incomingFormat", $"sensors.fields")

    val sensors = config.withColumn("sensors", explode($"sensors"))
      .select(columns: _*)

    sensors.registerTempTable("sensors")

    val sensorColumns = Seq(
      $"sensorName",$"fields.Name", $"fields.DisplayName", $"fields.DataType", $"fields.IncludeOnDashboard"
    )

    sensors.withColumn("fields", explode($"fields"))
      .select(sensorColumns: _*)
      .registerTempTable("sensorFields")
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseRead")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val tableName = "sensor"

    val JSONConfig = getJSONConfig(sqlContext)
    val fieldList = sqlContext.sql("SELECT sensorName, Name  " +
      "FROM sensorFields " +
      "WHERE sensorName = 'A1234' AND DataType = 'Number'")
      .rdd.map(r => r(1).asInstanceOf[String])
      .collect()
    val returnFields = List.fill(fieldList.length)("Option[Double]").mkString(", ")
    val stringFields = fieldList.mkString(", ")

    println(returnFields)
    println(stringFields)

    val sensorRDD = sc.hbaseTable[(String, Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double])](tableName)
      .select("temp", "humidity", "flo", "co2", "psi", "chlPPM")
      .inColumnFamily("data")

    sensorRDD.foreach(t => {
      if(t._2.nonEmpty) println(t)
    })
  }
}
