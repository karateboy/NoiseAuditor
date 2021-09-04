package models
import com.github.nscala_time.time.Imports._
import play.api._
import play.api.libs.json._

import scala.language.implicitConversions

/**
 * @author user
 */
import javax.inject._

object ModelHelper {
  implicit def getSqlTimestamp(t: DateTime) = {
    new java.sql.Timestamp(t.getMillis)
  }

  implicit def getDateTime(st: java.sql.Timestamp) = {
    new DateTime(st)
  }

  import org.mongodb.scala.bson.BsonDateTime
  implicit def toDateTime(time: BsonDateTime) = new DateTime(time.getValue)
  implicit def toBsonDateTime(jdtime: DateTime) = new BsonDateTime(jdtime.getMillis)

  def main(args: Array[String]) {
    val timestamp = DateTime.parse("2015-04-01")
    println(timestamp.toString())
  }

  def logException(ex: Throwable) = {
    Logger.error(ex.getMessage, ex)
  }
  def logInstrumentError(id: String, msg: String, ex: Throwable) = {
    Logger.error(msg, ex)
    //log(instStr(id), Level.ERR, msg)
  }

  def logInstrumentInfo(id: String, msg: String) = {
    Logger.info(msg)
    //log(instStr(id), Level.INFO, msg)
  }

  def errorHandler: PartialFunction[Throwable, Any] = {
    case ex: Throwable =>
      Logger.error("Error=>", ex)
      throw ex
  }

  def errorHandler(prompt: String = "Error=>"): PartialFunction[Throwable, Any] = {
    case ex: Throwable =>
      Logger.error(prompt, ex)
      throw ex
  }

  def windAvg(sum_sin: Double, sum_cos: Double) = {
    val degree = Math.toDegrees(Math.atan2(sum_sin, sum_cos))
    if (degree >= 0)
      degree
    else
      degree + 360
  }

  def windAvg(windSpeed: Seq[Record], windDir: Seq[Record]): Double = {
    if (windSpeed.length != windDir.length)
      Logger.error(s"windSpeed #=${windSpeed.length} windDir #=${windDir.length}")

    val windRecord = windSpeed.zip(windDir)
    val wind_sin = windRecord.map(v => v._1.value * Math.sin(Math.toRadians(v._2.value))).sum
    val wind_cos = windRecord.map(v => v._1.value * Math.cos(Math.toRadians(v._2.value))).sum
    windAvg(wind_sin, wind_cos)
  }

  def windAvg(windSpeed: List[Double], windDir: List[Double]): Double = {
    if (windSpeed.length != windDir.length)
      Logger.error(s"windSpeed #=${windSpeed.length} windDir #=${windDir.length}")

    val windRecord = windSpeed.zip(windDir)
    val wind_sin = windRecord.map(v => v._1 * Math.sin(Math.toRadians(v._2))).sum
    val wind_cos = windRecord.map(v => v._1 * Math.cos(Math.toRadians(v._2))).sum
    windAvg(wind_sin, wind_cos)
  }

  def getPeriods(start: DateTime, endTime: DateTime, d: Period): List[DateTime] = {
    import scala.collection.mutable.ListBuffer

    val buf = ListBuffer[DateTime]()
    var current = start
    while (current < endTime) {
      buf.append(current)
      current += d
    }

    buf.toList
  }

  import scala.concurrent._

  def waitReadyResult[T](f: Future[T]) = {
    import scala.concurrent.duration._
    import scala.util._

    val ret = Await.ready(f, Duration.Inf).value.get

    ret match {
      case Success(t) =>
        t
      case Failure(ex) =>
        Logger.error(ex.getMessage, ex)
        throw ex
    }
  }
}

object EnumUtils {
  def enumReads[E <: Enumeration](enum: E): Reads[E#Value] = new Reads[E#Value] {
    def reads(json: JsValue): JsResult[E#Value] = json match {
      case JsString(s) => {
        try {
          JsSuccess(enum.withName(s))
        } catch {
          case _: NoSuchElementException => JsError(s"Enumeration expected of type: '${enum.getClass}', but it does not appear to contain the value: '$s'")
        }
      }
      case _ => JsError("String value expected")
    }
  }

  implicit def enumWrites[E <: Enumeration]: Writes[E#Value] = new Writes[E#Value] {
    def writes(v: E#Value): JsValue = JsString(v.toString)
  }

  implicit def enumFormat[E <: Enumeration](enum: E): Format[E#Value] = {
    Format(enumReads(enum), enumWrites)
  }
}
