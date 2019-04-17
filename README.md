# hivetableutils
This has util classes which will help to create the hive table and drop them at the end of Spark Application

HiveTempTableCreatorImpl Class can be use to create Hive tables using spark Dataframe. While creating various options can be configured.
Currently below configurations are supported
 - format: Format of table, default value is parquet
 - Savemode: Drop the table at the end of application
 - droponApplicationEnd: Drop the table at the end of application
 
 How to use it
Ex.
 Assumsing spark is SparkSession variable
 
    val hiveTableDropListener: HiveTableDropListener = new HiveTableDropListener(spark)
    val hievTblCreator: HiveTempTableCreatorImpl = new HiveTempTableCreatorImpl(hiveTableDropListener)

    val sampleDF = Seq((1, "1"), (2, "2")).toDF("key", "value")
    // This will create table with above DF schema and values in parque format, if table is alredy present it will overwrite its data
    // true value specify that, drop this table at the end of spark application
    val tableParam: CreateTableInputParam = new CreateTableInputParam(sampleDF, "tableName", "db", true,"parquet", SaveMode.Overwrite)
    hievTblCreator.createTable(tableParam)
