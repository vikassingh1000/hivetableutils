package org.vik.spark.hive.utils

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.BeforeAndAfterAll

class HiveTempTableCreatorImplTest extends QueryTest with SharedSQLContext{

  import testImplicits._

  test("Create Hive Table") {
    val spark1 = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    val hiveTableDropListener: HiveTableDropListener = new HiveTableDropListener(spark1)
    val hiv: HiveTempTableCreatorImpl = new HiveTempTableCreatorImpl(hiveTableDropListener)

    val df = Seq((1, "1"), (2, "2")).toDF("key", "value")
    val tableParam: CreateTableInputParam = new CreateTableInputParam(df, "table", "db")
    hiv.createTable(tableParam)

  }
}
