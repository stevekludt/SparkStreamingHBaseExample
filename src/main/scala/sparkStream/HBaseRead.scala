package sparkStream

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import it.nerdammer.spark.hbase._
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
  * Created by stevekludt on 1/8/16.
  */

object HBaseRead {
  case class hbaseData(key: String, temp: Double, humidity: Double, flo: Double, co2: Double, psi: Double, chlPPM: Double)

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

  def getDFOfSensorData(args: Array[String], sc: SparkContext): DataFrame = {
    //val sparkConf = new SparkConf().setAppName("HBaseRead")
    //val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val tableName = "sensor"

    val JSONConfig = getJSONConfig(sqlContext)
    val fieldList = sqlContext.sql("SELECT sensorName, Name  " +
      "FROM sensorFields " +
      "WHERE sensorName = 'A1234' AND DataType = 'Number'")
      .rdd.map(r => r(1).asInstanceOf[String])
      .collect()
    val returnFields = List.fill(fieldList.length)("Option[Double]").mkString(", ")
    val stringFields = fieldList.mkString(" ")

    val sensorRDD = sc.hbaseTable[(String, Double, Double, Double, Double, Double, Double)](tableName)
      .select("temp", "humidity", "flo", "co2", "psi", "chlPPM")
      .inColumnFamily("data")
      .withStartRow("A1234")

    import sqlContext.implicits._
    import org.apache.spark.sql.types.{StructType,StructField,StringType,DoubleType, TimestampType}
    import org.apache.spark.sql.Row
    /*
    def parseID(id: String): Seq[String] = {
      val split = id.split("_")
      val sensorName = split(0)
      val timestamp = split(1)
      Seq(sensorName, timestamp)
    }
    //first create a new field for the ID of type string
    val hbaseKey = Seq(StructField("ID", StringType, true))

    //Assume all fields are of data type Double
    val fields = StructType(
      stringFields.split(" ").map(fieldName => StructField(fieldName, DoubleType, true))
    )
    //Concat the two schemas
    val dfSchema = StructType(hbaseKey ++ fields ++
        Array(StructField("SensorName", StringType), StructField("DateTime", StringType)))

    val HbaseData = sensorRDD.toJavaRDD().rdd
    val NameAndTS = HbaseData.map(r => parseID(r._1))


    //apply the Schema to the RDD Row
    val df = sqlContext.createDataFrame(HbaseData, dfSchema)
    */
    val getName = udf((id: String) => {
      val split = id.split("_")
      split(0)
    })
    val getTimeStamp = udf((id: String) => {
      val split = id.split("_")
      split(1)
    })

    val data = sensorRDD.toJavaRDD.rdd

    val df = data.toDF("ID", "temp", "humidity", "flo", "co2", "psi", "chlPPM")
    df.select($"*", getName($"ID").as("Name"), getTimeStamp($"ID").as("Timestamp"))
  }
}
