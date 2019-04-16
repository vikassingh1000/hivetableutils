package org.vik.spark.hive.utils

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Input param class, which will hold various value for Table creation
  *
  * @param dataFrame    - Dataframe which will be used to create Hive tables
  * @param tableName    - table name
  * @param db           - Database name
  * @param dropOnAppEnd - Drop the table at the end of application
  * @param format       - Format of table, default value if parquet
  * @param saveMode     - Save mode for Hive table creation, default value is overwrite
  */
case class CreateTableInputParam(dataFrame: DataFrame, tableName: String, db: String, dropOnAppEnd:
Boolean = true, format: String = "parquet", saveMode: SaveMode = SaveMode.Overwrite)
