package org.vik.spark.hive.utils

import org.apache.spark.sql.SparkSession

/**
  * Trait to create Table with given parameters
  */
trait HiveTempTableCreator {
  def createTable(param: CreateTableInputParam): Unit
}
