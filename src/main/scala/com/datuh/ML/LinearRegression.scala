package com.datuh.ML

import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import com.cloudera.sparkts._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.ml.feature.VectorAssembler
import com.datuh.sparkStream.HBaseRead

/**
  * Created by stevekludt on 2/28/16.
  */
object LinearRegression {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("LinearRegression")
    val sc = new SparkContext(sparkConf)

    val df = HBaseRead.getDFOfSensorData(Array[String](""), sc) //will eventually modify this to pass in a client or sensor
    df.show()

    /*
    val zone = ZoneId.systemDefault()
    val dtIndex = DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(LocalDateTime.parse("2016-01-01T00:00:00"), zone),
      ZonedDateTime.of(LocalDateTime.parse("2016-03-01T00:00:00"), zone),
      new MinuteFrequency(1))

    val tsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, df,
      "Timestamp", "humidity", "temp") //needs to be updated to "key", "value"
    */

    val assembler = new VectorAssembler()
      .setInputCols(Array("flo", "humidity", "co2", "psi", "chlPPM"))
      .setOutputCol("features")

    val output = assembler.transform(df)
    //val features = output.select("temp", "features")
    val trainingData = output.withColumn("label", output("temp"))
    trainingData.show()

    //val filled = tsrdd.fill("linear")

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      //.setPredictionCol("temp") //not sure if this works

    //fit model
    val lrModel = lr.fit(trainingData)

    println(s"Coefficients: ${lrModel} Intercept: ${lrModel}")

    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")





  }
}
