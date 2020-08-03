package com.dsm.utils

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3URI}
import com.amazonaws.services.s3.model.S3Object
import com.typesafe.config.{Config, ConfigFactory}

import scala.io.{BufferedSource, Source}

object ConfigUtil {

  def getApplicationConfig(): Config = {
    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")
    val credentials = new BasicAWSCredentials(s3Config.getString("access_key"), s3Config.getString("secret_access_key"))
    val s3Client = new AmazonS3Client(credentials)
    val uri: AmazonS3URI = new AmazonS3URI(s"s3://${Constants.S3_CONFIG_FOLDER}/application.conf")
    val s3Object: S3Object = s3Client.getObject(uri.getBucket, uri.getKey)
    val source: BufferedSource = Source.fromInputStream(s3Object.getObjectContent)
    try {
     val config= source.mkString
      ConfigFactory.parseString(config)
    } finally {
      source.close()
    }

  }

}
