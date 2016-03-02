package ML

import com.cloudera.sparkts._
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.util.MLUtils
import sparkStream.HBaseRead
import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.Vectors
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import org.apache.spark.sql._

/**
  * Created by stevekludt on 2/28/16.
  */
object LinearRegression {
  def main(args: Array[String]): Unit = {
    val df = HBaseRead.getDFOfSensorData(Array[String]("")) //will eventually modify this to pass in a client or sensor
    val zone = ZoneId.systemDefault()
    val dtIndex = DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(LocalDateTime.parse("2016-01-01T00:00:00"), zone),
      ZonedDateTime.of(LocalDateTime.parse("2016-03-01T00:00:00"), zone),
      new MinuteFrequency(1))

    df.show()

    val tsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, df,
      "Timestamp", "temp", "humidity")

    val filled = tsrdd.fill("linear")

    val splits = df.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFeaturesCol("humidity")

    //Fit the model
    val lrModel = lr.fit(df)

    println(s"Weights: ${lrModel.weights} Intercept: ${lrModel.intercept}")

    //summerize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
  }

}
