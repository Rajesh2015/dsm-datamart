package com.dsm

import java.text.SimpleDateFormat
import java.util.Calendar

import com.dsm.utils.{ConfigUtil, Constants, DsmUtility}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

object TargetDataLoading {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Taget Data Loading").getOrCreate()
    val rootConfig = ConfigUtil.getApplicationConfig().getConfig("conf")
    val srcList = rootConfig.getStringList("source_data_list").toList
    val s3Config = rootConfig.getConfig("s3_conf")
    val redshiftConfig = rootConfig.getConfig("redshift_conf")
    val targetList = rootConfig.getStringList("TARGET_LIST").toList
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))
    sparkSession.udf.register("FN_UUID", () => java.util.UUID.randomUUID().toString())

    for (target <- targetList) {
      val targetConf = rootConfig.getConfig(target)
      target match {
        case "REGIS_DIM" =>
          val cppath = s"${s3Config.getString("target_folder")}/${targetConf.getString("sourceFile")}"
          val cusDimdf = DsmUtility.readFromS3(sparkSession, s3Config, cppath, "parquet")
          cusDimdf.createOrReplaceTempView("1CP")
          val df = sparkSession.sql(targetConf.getString("loadingQuery"))
          df.show(false)
          DsmUtility.writeToRedshift(df.coalesce(1),
            s3Config.getString("s3_bucket"), redshiftConfig, targetConf.getString("tableName"))
        case "CHILD_DIM" =>
          val cppath = s"${s3Config.getString("target_folder")}/${targetConf.getString("sourceFile")}"
          val cusDimdf = DsmUtility.readFromS3(sparkSession, s3Config, cppath, "parquet")
          cusDimdf.createOrReplaceTempView("1CP")
          val df = sparkSession.sql(targetConf.getString("loadingQuery"))
          df.show(false)
          DsmUtility.writeToRedshift(df.coalesce(1),
            s3Config.getString("s3_bucket"), redshiftConfig, targetConf.getString("tableName"))

        case "RTL_TXN_FCT" =>
          val srcData = targetConf.getStringList("source_data").toList
          for (src <- srcData) {
            if (src.toUpperCase().endsWith("_DIM")) {
              val df = DsmUtility.readFromRedshift(sparkSession, s3Config, redshiftConfig, s"${rootConfig.getString("DATAMART_SCHEMA")}.${src}")
              df.createOrReplaceTempView(src)
            }
            else {
              val path = s"${s3Config.getString("target_folder")}/${src}"
              val df = DsmUtility.readFromS3(sparkSession, s3Config, path, "parquet")
              df.createOrReplaceTempView(src)
            }
          }
          val finalDF = sparkSession.sql(targetConf.getString("loadingQuery")).coalesce(1)
          DsmUtility.writeToRedshift(finalDF, s3Config.getString("s3_bucket"), redshiftConfig, targetConf.getString("tableName"))


      }
    }
    sparkSession.close()


  }

}
