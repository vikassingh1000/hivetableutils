# hivetableutils
This has util classes which will help to create the hive table and drop them at the end of Spark Application

HiveTempTableCreatorImpl Class can be use to create Hive tables using spark Dataframe. While creating various options can be configured.
Currently below configurations are supported
 - format: Format of table, default value if parquet
 - Savemode: Drop the table at the end of application
 - droponApplicationEnd: Drop the table at the end of application
 

