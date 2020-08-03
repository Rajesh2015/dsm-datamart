package com.dsm.utils

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DsmUtility {
  def readFromSftp(sparkSession: SparkSession, sftpConfig: Config, sftpPath: String): DataFrame = {
    sparkSession.read.
      format("com.springml.spark.sftp").
      option("host", sftpConfig.getString("hostname")).
      option("port", sftpConfig.getString("port")).
      option("username", sftpConfig.getString("username")).
      option("pem", sftpConfig.getString("pem")).
      option("fileType", "csv").
      option("delimiter", "|").
      load(sftpPath)
  }

  def readFromMySql(sparkSession: SparkSession, mysqlConfig: Config, mysqlTableName: String): DataFrame = {

    var jdbcParams = Map("url" -> getMysqlJdbcUrl(mysqlConfig),
      "lowerBound" -> "1",
      "upperBound" -> "100",
      "dbtable" -> mysqlTableName,
      "numPartitions" -> "2",
      "partitionColumn" -> "App_Transaction_Id",
      "user" -> mysqlConfig.getString("username"),
      "password" -> mysqlConfig.getString("password")
    )
    sparkSession
      .read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .options(jdbcParams) // options can pass map
      .load()

  }
  def getMysqlJdbcUrl(mysqlConfig: Config): String = {
    val host = mysqlConfig.getString("hostname")
    val port = mysqlConfig.getString("port")
    val database = mysqlConfig.getString("database")
    s"jdbc:mysql://$host:$port/$database?autoReconnect=true&useSSL=false"
  }
  def readFromS3(sparkSession: SparkSession, s3Config: Config, fileToRead: String,format:String):DataFrame={
    val path=s"s3n://${s3Config.getString("s3_bucket")}/${fileToRead}"
    format match{
      case "csv"=>
    sparkSession.read.option("delimiter", "|").option("header","true").csv(path)
      case "parquet"=>
        sparkSession.read.parquet(path)


    }


  }
  def getRedshiftJdbcUrl(redshiftConfig: Config): String = {
    val host = redshiftConfig.getString("host")
    val port = redshiftConfig.getString("port")
    val database = redshiftConfig.getString("database")
    val username = redshiftConfig.getString("username")
    val password = redshiftConfig.getString("password")
    s"jdbc:redshift://${host}:${port}/${database}?user=${username}&password=${password}"
  }
  def writeToRedshift(dataFrame:DataFrame,s3Bucket:String,redshiftConfig:Config,tablename:String): Unit =
  {
    dataFrame.write
      .format("com.databricks.spark.redshift")
      .option("url", getRedshiftJdbcUrl(redshiftConfig))
      .option("tempdir", s"s3n://${s3Bucket}/temp")
      .option("forward_spark_s3_credentials", "true")
      .option("dbtable",tablename)
      .mode(SaveMode.Overwrite)
      .save()

  }

  def readFromRedshift(sparkSession: SparkSession,s3Config:Config,redshiftConfig:Config,tablename:String):DataFrame={

    val s3Bucket = s3Config.getString("s3_bucket")
    sparkSession.read
      .format("com.databricks.spark.redshift")
      .option("url", getRedshiftJdbcUrl(redshiftConfig))
      .option("tempdir", s"s3n://${s3Bucket}/temp")
      .option("forward_spark_s3_credentials", "true")
      .option("dbtable", tablename)
      .load()
  }
}
