/**
  * Created by sulav on 12/2/16.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
object check {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("ImgClassification").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val train = sc.textFile("data/train")
    val parsedData = train.map { line =>
      val parts = line.split(',')


      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

    val numClasses = 4
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(parsedData, numClasses, categoricalFeaturesInfo,impurity, maxDepth, maxBins)
  }
}

