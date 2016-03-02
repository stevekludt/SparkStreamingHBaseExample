package ML

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.util.MLUtils
import sparkStream.HBaseRead
import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.Vectors
/**
  * Created by stevekludt on 2/28/16.
  */
object LinearRegression {
  def main(args: Array[String]): Unit = {
    val df = HBaseRead.getDFOfSensorData(Array[String]("")) //will eventually modify this to pass in a client or sensor
    df.show()
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
