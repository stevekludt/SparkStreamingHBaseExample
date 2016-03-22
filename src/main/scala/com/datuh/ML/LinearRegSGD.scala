package com.datuh.ML

import com.datuh.Utils.DateTimeUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.datuh.sparkStream.HBaseRead

/**
  * Created by stevekludt on 3/12/16.
  */
object LinearRegSGD {

  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("LinearRegSGD")
    val sc = new SparkContext(sparkConf)

    val df = HBaseRead.getDFOfSensorData(Array[String](""), sc)
    df.show()

    val data = df.select("temp", "Timestamp", "ID").rdd.map { row =>
      val dtLength = row(1).toString.length
      val date = row(1).toString.substring(0, 10)
      val time = row(1).toString.substring(12, dtLength)
      val datetime = DateTimeUtils.getDateforML(date, time)
      LabeledPoint(row(0).toString.toDouble, datetime)
    }.cache()
    data.foreach(println(_))

    // Building the model
    val numIterations = 100
    val stepSize = 0.000001
    val regParam = 0.01
    val regType = "L2"

    val model = LinearRegressionWithSGD.train(data, numIterations)

    val algorithm = new LinearRegressionWithSGD()
    algorithm.optimizer
      .setNumIterations(numIterations)
      .setStepSize(stepSize)
      .setRegParam(regParam)

    val model2 = algorithm.run(data)


    val valuesAndPreds = data.map {point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val valuesAndPreds2 = data.map {point =>
      val prediction = model2.predict(point.features)
      (point.label, prediction)
    }
    valuesAndPreds.foreach(println(_))

    valuesAndPreds2.foreach(println(_))

    val MSE = valuesAndPreds.map{case(v, p) => math.pow(v - p, 2)}
    println("training Mean Squared Error = " + MSE)

  }

}
