package org.vik.spark.hive.utils

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
  * Implementation class of org.vik.spark.hive.utils.HiveTempTableCreator trait
  *
  * @param hiveTableDropListener
  */
class HiveTempTableCreatorImpl(hiveTableDropListener: HiveTableDropListener) extends HiveTempTableCreator {

  private[this] val logger = Logger.getLogger(getClass().getName());

  override def createTable(param: CreateTableInputParam): Unit = {

    val tableFQDN = param.db + "." + param.tableName
    createHiveTbleAndRegisterForDrop(param, param.tableName)

  }

  private def createHiveTbleAndRegisterForDrop(param: CreateTableInputParam, tableFQDN: String) = {
    try {
      createHiveTable(param, tableFQDN)
      registerForDrop(param, tableFQDN)
    }
    catch {
      case e: Exception =>
        logger.error(s"Fail to create Table $tableFQDN", e)
    }
  }

  private def registerForDrop(param: CreateTableInputParam, tableFQDN: String) = {
    if (param.dropOnAppEnd) {
      hiveTableDropListener.addHiveTableForRemove(tableFQDN)
      logger.info("Hive table registered for drop, action would be carried out at end of Application!!!!!!")
    }
  }

  private def createHiveTable(param: CreateTableInputParam, tableFQDN: String) = {
    logger.info(s"Creating Hive table $tableFQDN using following parameters:" + param.toString)
    param.dataFrame.write.mode(param.saveMode).format(param.format).saveAsTable(tableFQDN)
    logger.info("Creating Hive table created!!!!!!!!!!!")
  }
}
