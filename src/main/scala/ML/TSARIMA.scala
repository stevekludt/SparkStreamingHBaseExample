package ML

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import com.github.nscala_time.time.Imports._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import sparkStream.HBaseRead
import com.cloudera.sparkts._
import com.cloudera.sparkts.models.ARIMA
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by stevekludt on 3/17/16.
  */
object TSARIMA {

  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("RunARIMAModel")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = HBaseRead.getDFOfSensorData(Array[String](""), sc)

    val getTimeStamp = udf((ts: String) => {
      val tsArray = ts.split("T")
      val formatter = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss")
      new java.sql.Timestamp(formatter.parseDateTime(tsArray(0) + " " + tsArray(1)).getMillis)
    })

    val dfWithTS = df.select($"*", getTimeStamp($"Timestamp").as("TS"))
    dfWithTS.show()

    val zone = ZoneId.systemDefault()
    val dtIndex = DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(LocalDateTime.parse("2016-03-14T09:00:00"), zone),
      ZonedDateTime.of(LocalDateTime.parse("2016-03-14T09:20:00"), zone),
      new SecondFrequency(1))

    val sensorTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, dfWithTS,
      "TS", "Name", "temp")

    sensorTsrdd.cache()

    //println(sensorTsrdd.count())
    val filled: TimeSeriesRDD[String] = sensorTsrdd.fill("linear").removeInstantsWithNaNs()

    val ts = filled.toDF().select("_2").map { row =>
      val toRemove = "[]".toSet
      val darray = row.toString.filterNot(toRemove).split(",").map(_.toDouble)
      Vectors.dense(darray)
    }
    //val ts2 = ts.select(ts("_2")).rdd
    val ARIMAModels = ts.foreach{ vector =>
      val arimaModel = ARIMA.autoFit(vector)
      println("coefficients: " + arimaModel.coefficients.mkString(","))
      val forecast = arimaModel.forecast(vector, 20)
      println("forecast of next 20 observations: " + forecast.toArray.mkString(","))

    }
    println("Here are the models: " + ARIMAModels)
  }

}
