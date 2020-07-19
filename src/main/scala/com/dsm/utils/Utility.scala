package com.dsm.utils

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

object Utility {
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

}
