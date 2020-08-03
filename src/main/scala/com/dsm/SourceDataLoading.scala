package com.dsm


import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.dsm.utils._
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

object SourceDataLoading {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Source Data Loading").getOrCreate()
    val rootConfig = ConfigUtil.getApplicationConfig().getConfig("conf")
    val srcList = rootConfig.getStringList("source_data_list").toList
    val s3Config = rootConfig.getConfig("s3_conf")
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    //LoggingUtil.scanTable(s3Config,"Statistics")
    for (src <- srcList) {
      lazy val startDateTime: Long = System.currentTimeMillis()
      try {

        val srcConfig = rootConfig.getConfig(src)
        val jobId = sparkSession.sparkContext.applicationId
        println(classOf[TableDescription].getProtectionDomain.getCodeSource)
        LoggingUtil.logToCheckPoint(s3Config, src, jobId,
          Status.STARTED, TimeUtils.millsToLocalDateTime(startDateTime).toString)
        src match {
          case "OL" =>
            val sftpConfig = srcConfig.getConfig("sftp_conf")
            val filename = srcConfig.getString("filename")
            // log this into a redshift table withh status "Started"
            val olTxnDf = DsmUtility.readFromSftp(sparkSession, sftpConfig,
              s"${sftpConfig.getString("directory")}/$filename")
              .withColumn("ins_ts", current_date())
            olTxnDf.show()
            olTxnDf.write.mode("overwrite").partitionBy("Ins_Ts").parquet(s"s3n://${s3Config.getString("s3_bucket")}/datamart/${src}")
            // update te statusto succeeded
            val srcWithFileName = s"${src}(${filename})"
            LoggingUtil.updateStatusAndLogToStatistics(s3Config, startDateTime, jobId, olTxnDf.count(), srcWithFileName)
          // log to statistics table
          case "SB" =>
            val mysqlConfig = srcConfig.getConfig("mysql_conf")
            val tableName = srcConfig.getString("table_to_load")
            val sbTxnDf = DsmUtility.readFromMySql(sparkSession, mysqlConfig, tableName)
              .withColumn("ins_ts", current_date())
            sbTxnDf.show()
            sbTxnDf.write.mode("overwrite").partitionBy("Ins_Ts").parquet(s"s3n://${s3Config.getString("s3_bucket")}/datamart/${src}")
            val srcWithFileName = s"${src}(${tableName})"
            LoggingUtil.updateStatusAndLogToStatistics(s3Config, startDateTime, jobId, sbTxnDf.count(), srcWithFileName)

          case "1CP" =>
            val s3Conf = srcConfig.getConfig("s3_conf")
            val fileToRead = srcConfig.getString("file_to_read")
            val cpDf = DsmUtility.readFromS3(sparkSession, s3Conf, fileToRead,"csv").withColumn("Ins_Ts", current_date())
            cpDf.show()
            cpDf.write.mode(SaveMode.Overwrite).partitionBy("Ins_Ts").parquet(s"s3n://${s3Config.getString("s3_bucket")}/datamart/${src}")
            val srcWithFileName = s"${src}(${fileToRead})"
            LoggingUtil.updateStatusAndLogToStatistics(s3Config, startDateTime, jobId, cpDf.count(), srcWithFileName)
        }
      } catch {
        case ex: Throwable => {
          ex.printStackTrace()
          LoggingUtil.updateCheckPoint(s3Config,
            Status.FAILED, TimeUtils.millsToLocalDateTime(startDateTime).toString)
        }
      }
    }
    sparkSession.close()

  }


}
