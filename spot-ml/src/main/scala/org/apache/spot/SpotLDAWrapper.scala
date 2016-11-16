package org.apache.spot

import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spot.{SpotLDACWrapper, SpotSparkLDAWrapper}
import scala.io.Source._
import scala.sys.process._
import org.apache.log4j.Logger

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

  case class SpotLDAOutput(docToTopicMix: Map[String, Array[Double]], wordResults: Map[String, Array[Double]])

  case class InvalidLDAImp(message: String) extends Exception(message)

  def runLDA(docWordCount: RDD[SpotLDAInput],
             modelFile: String,
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
             prgSeed: Option[Long]):   SpotLDAOutput =  {

    val spotOutput = ldaImp match {
      case "LDAC" => SpotLDACWrapper.runLDA(docWordCount,
        modelFile,
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
        prgSeed)
      case "SparkLDA" => SpotSparkLDAWrapper.runLDA(docWordCount,
        topicCount,
        logger,
        prgSeed,
        ldaOptimizer,
        ldaAlpha,
        ldaBeta,
        maxIterations)
      case _ => throw InvalidLDAImp(s"Invalid LDA implementation specified, options are LDAC or SparkLDA")
    }

    spotOutput
  }

}



