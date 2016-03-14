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

    val predictWithLabels = stream.map{x =>
      val label = x._1
      val features = DateTimeUtils.addMinutes(x._2, FutureMinToPredict)
      LabeledPoint(label, features)
    }

    val numFeatures = 1
    val iterations = 100
    val stepSize = 0.001

    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.zeros(numFeatures))
      .setNumIterations(iterations)
      .setStepSize(stepSize)

    model.trainOn(trainingData)
    //model.predictOn(predictionData).print()
    model.predictOnValues(predictWithLabels.map(lp => (lp.label, lp.features))).print()

    println(trainingData)
  }

}
