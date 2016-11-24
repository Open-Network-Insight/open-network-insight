package org.apache.spot

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * Spot LDA Overall Wrapper
  * Wraps SpotSparkLDAWrapper and SpotLDACWrapper so that there can be a single interface for flow, dns, and proxy
  * 1. SpotLDAWrapper input and output have the same components as SpotLDACWrapper and SpotSparkWrapper
  * 2. Determines which version of LDA (LDAC or Spark) to call based on the input parameter, ldaImplemention
  * 3. Converts SpotLDAWrapper input into correct version and calls appropriate version of runLDA
  * 4. Converts output to generic SpotLDAOutput for further processing
  */

object SpotLDAWrapper {


  case class SpotLDAInput(doc: String, word: String, count: Int) extends Serializable

  case class SpotLDAOutput(docToTopicMix: DataFrame, wordResults: Map[String, Array[Double]])


  case class InvalidLDAImp(message: String) extends Exception(message)

  def runLDA(sparkContext: SparkContext,
             sqlContext: SQLContext,
              docWordCount: RDD[SpotLDAInput],
             modelFile: String,
             hdfsModelFile: String,
             topicDocumentFile: String,
             topicWordFile: String,
             mpiPreparationCmd: String,
             mpiCmd: String,
             mpiProcessCount: String,
             topicCount: Int,
             localPath: String,
             ldaPath: String,
             localUser: String,
             dataSource: String,
             nodes: String,
             ldaImp: String,
             logger: Logger,
             ldaOptimizer: String,
             ldaAlpha: Double,
             ldaBeta: Double,
             maxIterations: Int,
             prgSeed: Option[Long]): SpotLDAOutput = {

    val spotOutput = ldaImp match {
      case "LDAC" => {
        logger.info("\nYou are so old school, you run    LDAC\n")

        spotldacwrapper.SpotLDACWrapper.runLDA(docWordCount,
          modelFile,
          hdfsModelFile,
          topicDocumentFile,
          topicWordFile,
          mpiPreparationCmd,
          mpiCmd,
          mpiProcessCount,
          topicCount,
          localPath,
          ldaPath,
          localUser,
          dataSource,
          nodes,
          prgSeed,
          sparkContext,
          sqlContext,
          logger
        )

      }
      case "SparkLDA" => {
        logger.info("\nYou are with the fresh hotness that is    SparkLDA\n")
        SpotSparkLDAWrapper.runLDA(
          sparkContext: SparkContext,
          sqlContext: SQLContext,
          docWordCount,
          topicCount,
          logger,
          prgSeed,
          ldaOptimizer,
          ldaAlpha,
          ldaBeta,
          maxIterations)
      }
      case _ => throw InvalidLDAImp(s"Invalid LDA implementation specified, options are LDAC or SparkLDA")
    }

    spotOutput
  }

}



