package com.datuh.ML

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import com.github.nscala_time.time.Imports._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import com.datuh.sparkStream.HBaseRead
import com.cloudera.sparkts._
import com.cloudera.sparkts.models.ARIMA
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by stevekludt on 3/17/16.
  */
object TimeSeriesARIMA {

  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("RunARIMAModel")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Get data from HBase to build model from
    val df = HBaseRead.getDFOfSensorData(Array[String](""), sc)

    // A UDF to parse the String DateTime to java TimeStamp
    val getTimeStamp = udf((ts: String) => {
      val tsArray = ts.split("T")
      val formatter = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss")
      new java.sql.Timestamp(formatter.parseDateTime(tsArray(0) + " " + tsArray(1)).getMillis)
    })

    // Append the DateTime in Timestamp format to the DF
    val dfWithTS = df.select($"*", getTimeStamp($"Timestamp").as("TS"))
    dfWithTS.show()

    // Set the range (in time) of the values we are going to use to train the model
    val zone = ZoneId.systemDefault()
    val dtIndex = DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(LocalDateTime.parse("2016-03-14T09:00:00"), zone),
      ZonedDateTime.of(LocalDateTime.parse("2016-03-14T10:20:00"), zone),
      new SecondFrequency(1))

    // Map the Data into a TimeSeries
    val sensorTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, dfWithTS,
      "TS", "Name", "temp")

    sensorTsrdd.cache()

    // create a value for every second using the average between values
    val filled = sensorTsrdd.fill("linear").removeInstantsWithNaNs()

    // Create a vector of the filled values to use for modeling
    val ts = filled.toDF().select("_2").map { row =>
      val toRemove = "[]".toSet
      val darray = row.toString.filterNot(toRemove).split(",").map(_.toDouble)
      Vectors.dense(darray)
    }

    // Train the model for each sensor and forecast the next intervals
    val ARIMAModels = ts.foreach{ vector =>
      val arimaModel = ARIMA.autoFit(vector)
      println("coefficients: " + arimaModel.coefficients.mkString(","))
      val forecast = arimaModel.forecast(vector, 20)
      println("forecast of next 20 observations: " + forecast.toArray.mkString(","))
      println(arimaModel)

    }
    println("Here are the models: " + ARIMAModels)
  }
}
//coefficients: 0.9087073500233678,1.8527880948109496,-0.8701546853124326,0.011133353570422617,0.01894204501405337
//coefficients: 1.8607968775044934,-0.8843115987450779,0.0015504062344943597,-0.9937479048913188
//coefficients: 1.2816353259650402,1.8623598872883076,-0.8865659295384433,-0.03183158463918353,-0.013113617168474303
//coefficients: 0.8388068636728792,0.0019693331733664932,0.07654343313352503