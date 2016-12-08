/**
  * Created by sulav on 12/2/16.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object Image_DT {
  def main(args: Array[String]) {
    //val IMAGE_CATEGORIES = Array("Horse", "Bird", "SeaLion", "Swan", "Beaver")
    //System.setProperty("hadoop.home.dir", "D:\\winutils")
    //    Logger.getLogger("org").setLevel(Level.ERROR)
    //    Logger.getLogger("akka").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("ImgClassification").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val train = sc.textFile("data/train")
    val test = sc.textFile("data/test")
    val parsedData = train.map { line =>
      val parts = line.split(',')

      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }
    val testData1 = test.map(line => {
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    })

    val trainingData = parsedData


    val numClasses = 3
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    val classify1 = testData1.map { line =>
      val prediction = model.predict(line.features)
      (line.label, prediction)
    }

    val metrics1 = new MulticlassMetrics(classify1)

    println("Accuracy: " + metrics1.accuracy)
    println("Confusion Matrix: \n" + metrics1.confusionMatrix)
    scala.tools.nsc.io.File("tree.txt").writeAll(model.toDebugString)

  }
}

