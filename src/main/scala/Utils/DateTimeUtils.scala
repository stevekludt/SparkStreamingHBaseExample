package Utils

import java.text.SimpleDateFormat
import java.util.Date
import com.github.nscala_time.time.Imports
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import com.github.nscala_time.time.Imports._

/**
  * Created by stevekludt on 3/7/16.
  */
object DateTimeUtils {
  def toDate(date: String, time: String): DateTime = {
    DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss").parseDateTime(date + " " + time)
  }

  def getDateforML(date: String, time: String): linalg.Vector = {
    val dateTime = toDate(date, time)
    val year = dateTime.getYear.toDouble
    val month = dateTime.getMonthOfYear.toDouble
    val day = dateTime.getDayOfMonth.toDouble
    val hour = dateTime.getHourOfDay.toDouble
    val minute = dateTime.getMinuteOfHour.toDouble
    val second = dateTime.getSecondOfMinute

    Vectors.dense(year, month, day, hour, minute, second)
  }

  def getDateMap(dateTime: DateTime) = {
    // create the date/time formatters
    val year = dateTime.getYear.toDouble
    val month = dateTime.getMonthOfYear.toDouble
    val day = dateTime.getDayOfMonth.toDouble
    val hour = dateTime.getHourOfDay.toDouble
    val minute = dateTime.getMinuteOfHour.toDouble
    val second = dateTime.getSecondOfMinute

    (year, month, day, hour, minute, second)
  }

  def addMinutes(dateTime: Imports.DateTime, NumMinutes: Int): linalg.Vector = {
    //convert from Date to Joda DateTime
    val newDateTime = dateTime + NumMinutes.minutes  //need to add NumMinutes
    val dateMap = getDateMap(newDateTime)

    Vectors.dense(dateMap._1, dateMap._2, dateMap._3, dateMap._4, dateMap._5, dateMap._6)
  }

}
