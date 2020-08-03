package com.dsm.utils

import java.time.{Instant, LocalDateTime, ZoneId}

object TimeUtils {
  def millsToLocalDateTime(millis: Long): LocalDateTime = {
    val instant = Instant.ofEpochMilli(millis)
    val date = instant.atZone(ZoneId.systemDefault).toLocalDateTime
    date
  }

  def miliSecondsToReadable(milisecond: Long): String = {
    val millis = 29184;
    val seconds = millis / 1000;
    val minutes = seconds / 60;
    val hours = minutes / 60;
    val days = hours / 24;
    s"${hours % 24}h:${minutes % 60}m:${seconds % 60}s";

  }
}
