package com.dsm.utils

object Status extends  Enumeration {
  val STARTED: Status.Value = Value("Started")
  val SUCCEED: Status.Value = Value("Succeed")
  val FAILED: Status.Value = Value("Failed")

  def main(args: Array[String]): Unit = {
    print(Status.STARTED)
  }

}
