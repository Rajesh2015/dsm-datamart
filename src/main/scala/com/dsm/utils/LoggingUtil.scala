package com.dsm.utils



import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item}
import com.amazonaws.services.dynamodbv2.document.spec.{ScanSpec, UpdateItemSpec}
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.model.ReturnValue
import com.typesafe.config.Config


object LoggingUtil {
  def logToCheckPoint(s3Config:Config,source:String,jobId:String,status:Status.Value,date:String):Unit={
    val awsCreds = new BasicAWSCredentials(s3Config.getString("access_key"), s3Config.getString("secret_access_key"))
    val client = AmazonDynamoDBClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(Regions.EU_WEST_1)
      .build();
    val dynamoDB = new DynamoDB(client)
    val table = dynamoDB.getTable("CheckPoint")
    val item=new Item()
    val outcome=table.putItem(item.withPrimaryKey("Date",date).withString("JobId",jobId)
      .withString("Source",source).withString("StatusOfJob",status.toString))
    println(s"InsertItem in checkpoint succeeded:${outcome}")
    client.shutdown()
  }
  def updateCheckPoint(s3Config:Config,status:Status.Value,date:String):Unit={
    val awsCreds = new BasicAWSCredentials(s3Config.getString("access_key"), s3Config.getString("secret_access_key"))
    val client = AmazonDynamoDBClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(Regions.EU_WEST_1)
      .build();
    val dynamoDB = new DynamoDB(client)
    val table = dynamoDB.getTable("CheckPoint")
    val updateItemSpec = new UpdateItemSpec().withPrimaryKey("Date", date)
      .withUpdateExpression("set StatusOfJob = :s")
      .withValueMap(new ValueMap().withString(":s", status.toString))
      .withReturnValues(ReturnValue.UPDATED_NEW);
    val outcome = table.updateItem(updateItemSpec)
    println(s"UpdateItem succeeded:${outcome}")
    client.shutdown()
  }
  def logToStatisticsTable(s3Config:Config, source:String, jobId:String, noOfRecords:Long, startTime:Long, endtime:Long):Unit={
    val duration=endtime-startTime
    val awsCreds = new BasicAWSCredentials(s3Config.getString("access_key"), s3Config.getString("secret_access_key"))
    val client = AmazonDynamoDBClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(Regions.EU_WEST_1)
      .build();
    val dynamoDB = new DynamoDB(client)
    val table = dynamoDB.getTable("Statistics")
    val item=new Item()
    table.putItem(item.withPrimaryKey("Date",TimeUtils.millsToLocalDateTime(startTime).toString)
      .withString("JobId",jobId)
      .withString("Source",source)
      .withLong("No Of Records",noOfRecords)
      .withString("Duration",TimeUtils.miliSecondsToReadable(startTime)))
    client.shutdown()

  }
  def updateStatusAndLogToStatistics(s3Config: Config, startDateTime: => Long, jobId: String, numberofrecords: Long, srcWithFileName: String) = {
    LoggingUtil.updateCheckPoint(s3Config,
      Status.SUCCEED, TimeUtils.millsToLocalDateTime(startDateTime).toString)
    LoggingUtil.logToStatisticsTable(s3Config, srcWithFileName, jobId, numberofrecords, startDateTime, System.currentTimeMillis())
  }
  def scanTable(s3Config:Config,tableName:String):Unit={
    val awsCreds = new BasicAWSCredentials(s3Config.getString("access_key"), s3Config.getString("secret_access_key"))
    val client = AmazonDynamoDBClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(Regions.EU_WEST_1)
      .build();
    val dynamoDB = new DynamoDB(client)
    val table = dynamoDB.getTable(tableName)
    val scanSpec = new ScanSpec()
    val items = table.scan(scanSpec);
    val iterator = items.iterator()
    while (iterator.hasNext) {
      val item = iterator.next
      println(item.toString)
    }
    client.shutdown()

  }
}
