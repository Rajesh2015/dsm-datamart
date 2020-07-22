package com.dsm

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.dsm.utils._
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

object SourceDataLoading {
  def main(args: Array[String]): Unit = {
    try {
      val sparkSession = SparkSession.builder.master("local[*]")
        .appName("Dataframe Example").getOrCreate()
      val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
      val srcList = rootConfig.getStringList("source_data_list").toList
      val s3Config = rootConfig.getConfig("s3_conf")
      sparkSession.sparkContext.setLogLevel(Constants.ERROR)
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId",s3Config.getString("access_key"))
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey",s3Config.getString("secret_access_key"))
      for(src <- srcList) {
        val srcConfig = rootConfig.getConfig(src)
        val sftpConfig = srcConfig.getConfig("sftp_conf")

        src match {
          case "OL" =>
            // log this into a redshift table withh status "Started"
            val olTxnDf = Utility.readFromSftp(sparkSession, sftpConfig,
              s"${sftpConfig.getString("directory")}/${srcConfig.getString("filename")}")
              .withColumn("ins_ts", current_date())
            olTxnDf.show()
            olTxnDf.write.mode("overwrite").partitionBy("Ins_Ts").parquet(s"s3n://${s3Config.getString("s3_bucket")}/datamart")
            // update te statusto succeeded
            // log to statistics table
          case "SB" =>

        }
      }
      sparkSession.close()
    } catch {
      case ex: Throwable => {
        ex.printStackTrace()
        //  update status to failed

      }
    }
  }


}
