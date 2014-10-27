import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import scala.io.Source
import scala.collection.mutable.Map


import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.optimization.{SimpleUpdater, SquaredL2Updater, L1Updater}

// Logistic Regression based classifier
// on sample data set`
object lgclassbank {
      
    def loadbankFile(sc: SparkContext, filename:String): RDD[LabeledPoint] = {
        val filename  =  "hdfs://priya-Aspire-V3-571:54310/tmp/rowdir"
        val data = sc.textFile(filename)
        val parsedata = data.map(_.trim).map  { rawline =>
                          val line = rawline.replace("[", "").replace("]","")
                          val items = line.split(',').map(_.toDouble)
                          val N = items.length
                          val label = items(N-1)
                          val arr = items.slice(0, N-1)
                          LabeledPoint(label, Vectors.dense(arr))
                          }
        parsedata
      }


    def getModelScore(train_data:RDD[LabeledPoint], test_data:RDD[LabeledPoint], configDict: Map[String, String]) = {
		val algorithm = new LogisticRegressionWithLBFGS()
		//val algorithm = new LogisticRegressionWithSGD()
		val regParam  = configDict("regParam").toDouble
		val numIterations = configDict("numIterations").toInt
		val stepSize = configDict("stepSize").toDouble
	  	val updater = configDict("penaltyFunc") match {
		      case "L1" => new L1Updater()
		      case "L2" => new SquaredL2Updater()
	    }
		val numFolds = configDict("numFolds").toInt

		algorithm.optimizer
				.setNumIterations(numIterations)
 				.setUpdater(updater)
		        .setRegParam(regParam)
		        .setStepSize(stepSize)

		val model = algorithm.run(train_data)
		val valuesAndPreds = test_data.map { point =>
			val prediction = model.predict(point.features)
		    (point.label, prediction)
		}

		val metrics = new BinaryClassificationMetrics(valuesAndPreds)
	    val auROC = metrics.areaUnderROC()
    	auROC
	}


      def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
        val p = new java.io.PrintWriter(f)
        try { op(p) } finally { p.close() }
      }


      def readConfigFile(filename: String): Map[String, String] = {
        var configParams  = Map[String, String]()
        for (line <- Source.fromFile(filename).getLines()) {
          val spltstg = line.trim().split("=")
          configParams(spltstg(0))  =  spltstg(1)
        }
        configParams
      } 
      


      def main(args: Array[String]) {

          // Creating a spark context 
          val conf = new SparkConf().setAppName("Logistic Application")
          val sc = new SparkContext(conf)

          // Read the config File 
          val configDict = readConfigFile("config.properties")
          val srcfilename = "hdfs://priya-Aspire-V3-571:54310" + configDict("samplefile")
          val data = loadbankFile(sc, srcfilename).cache

          // kFold Cross Validataion Data
          // numFolds
          //  Split data into training (60%) and test (40%).
          val numFolds = configDict("numFolds").toInt 
          val cvdata = MLUtils.kFold( data, numFolds, 0)
          val aucvec  = cvdata.map { dataset => 
          				val train_data = dataset._1
          				val test_data = dataset._2
          				val aucscore =  getModelScore(train_data, test_data, configDict)
          				println( "aucscore is " + aucscore) 
          				aucscore
          			}


           printToFile(new File("example.txt")) { p =>
           		aucvec.foreach( x => p.println(x))
           }

           
      }
}
