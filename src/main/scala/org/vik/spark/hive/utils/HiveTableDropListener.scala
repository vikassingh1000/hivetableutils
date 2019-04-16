package org.vik.spark.hive.utils

import org.apache.log4j.Logger
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * Below is the responsibility of the this clas
  *  - Expose method to add hive table details name for deletion
  *  - override onApplicationEnd and delete all register methods
  *
  * @param sqlSession
  */
class HiveTableDropListener(sqlSession: SparkSession) extends SparkListener {

  private[this] val logger = Logger.getLogger(getClass().getName());

  private val hiveTablesToBeRemoved: scala.collection.mutable.ListBuffer[String] = ListBuffer()

  def addHiveTableForRemove(tableFQDN: String): Unit = {
    hiveTablesToBeRemoved += tableFQDN
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    hiveTablesToBeRemoved.foreach(tbl => dropHiveTable(tbl))
    super.onApplicationEnd(applicationEnd)
  }

  private def dropHiveTable(tbl: String) = {
    try {
      sqlSession.sql(s"drop table $tbl")
      logger.info(s"Table $tbl dropped")
    }
    catch {
      case e: Exception =>
        logger.error(s"Fail to drop table $tbl", e)
    }
  }
}
