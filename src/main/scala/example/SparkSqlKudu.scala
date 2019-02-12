package example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.kudu.spark.kudu._

object SparkSqlKudu extends LogHelper {

  val kuduMasterConstant = "spark.example.kudu.masters"
  val yamlHdfsPathConstant = "spark.example.yaml.path"
  val outputDatabaseConstant = "spark.example.yaml.path"

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("spark-sql-kudu")
      .enableHiveSupport()
      .config("spark.sql.hive.caseSensitiveInferenceMode", "INFER_ONLY")
      .getOrCreate

    spark.sparkContext.setLogLevel("INFO")

    val kuduMasters = spark.sparkContext.getConf.get(kuduMasterConstant)
    val yamlHdfsPath = spark.sparkContext.getConf.get(yamlHdfsPathConstant)
    val outputDatabase = spark.sparkContext.getConf.get(outputDatabaseConstant)

    val yamlString = spark.sparkContext.textFile(yamlHdfsPath).collect.mkString("\n")
    val listOfViews = YamlConfigParser.convert(yamlString)

    listOfViews.all_queries.foreach(x => {
      stageTempViews(x.kudu_tables, kuduMasters)
      writeData(runQueryGetData(x.query),x.name, outputDatabase)
    })

  }


  /** Stages Kudu tables for use in the sqlContext
    *
    * @param tables List of Kudu tables as strings
    * @param kuduMasters Kudu master nodes
    */
  def stageTempViews(tables: List[String], kuduMasters: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    tables.par.foreach(table => {
      spark.sparkContext.setJobDescription(s"Staging table $table")
      log.info(s"Staging tables for $table")

      spark.read.options(Map("kudu.master" -> kuduMasters, "kudu.table" -> table))
        .kudu
        .createOrReplaceTempView(table)

    })
  }


  /** Drop & create Hive tables
    *
    * @param df   Dataframe of data to write
    * @param name Name of the destination Hive table
    * @param database Destination database
    */
  def writeData(df: DataFrame, name: String, database: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setJobDescription(s"Writing $name table to HDFS & Creating in Hive ")

    df.createOrReplaceTempView(name)

    spark.sql(s"DROP TABLE IF EXISTS $database.$name ")

    spark.sql(s"CREATE EXTERNAL TABLE $database.$name " +
      s"STORED AS PARQUET LOCATION '$database/$name' " +
      s"AS SELECT * FROM $name ")

  }


  /** Execute the query
    *
    * @param sql Query to run
    */
  def runQueryGetData(sql: String): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()

    log.info(s"APP - QUERY IS:\n $sql")

    spark.sql(sql)

  }

}

