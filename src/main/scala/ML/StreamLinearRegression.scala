package ML

/**
  * Created by stevekludt on 3/6/16.
  */

import java.util.Date
import com.github.nscala_time.time.Imports
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{StreamingLinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import Utils.DateTimeUtils



object StreamLinearRegression {
  def streamPredict(ssc: StreamingContext, stream: DStream[(Double, Imports.DateTime, linalg.Vector)]): Unit = {
    val trainingData = stream.map{x =>
      val label = x._1
      val features = x._3
      LabeledPoint(label, features)
    }.cache()

    val FutureMinToPredict = 5

    val predictionData = stream.map{x =>
      DateTimeUtils.addMinutes(x._2, FutureMinToPredict)
    }

    val numFeatures = 6

    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.zeros(numFeatures))

    model.trainOn(trainingData)
    model.predictOn(predictionData).print()

    println(trainingData)
  }

}
