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

    listOfViews.all_queries.foreach(x => runQuery(x, kuduMasters, outputDatabase))

  }


  /** Stages Kudu tables for use in the sqlContext
    *
    * @param viewDetails Case Class for a give materialized view
    * @param kuduMasters Kudu master nodes
    */
  def stageTempViews(viewDetails: Query, kuduMasters: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setJobDescription(s"Staging tables for ${viewDetails.name}")

    viewDetails.kudu_tables.foreach(x => {
      spark.read.options(Map("kudu.master" -> kuduMasters, "kudu.table" -> s"impala::latitude_us.$x")).kudu
        .createOrReplaceTempView(x)

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

    spark.sql(s"CREATE EXTERNAL TABLE heartlogic.$name " +
      s"STORED AS PARQUET LOCATION '$database/$name' " +
      s"AS SELECT * FROM $name ")

  }


  /** Stage, execute, and write the query results
    *
    * @param viewDetails Case Class for a given materialized view
    * @param kuduMasters Kudu master nodes
    * @param database Destination database
    */
  def runQuery(viewDetails: Query, kuduMasters: String, database: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    stageTempViews(viewDetails, kuduMasters)

    spark.sparkContext.setJobDescription(s"Running query for ${viewDetails.name}")

    val df = spark.sql(viewDetails.query)

    writeData(df, viewDetails.name, database)

  }

}

