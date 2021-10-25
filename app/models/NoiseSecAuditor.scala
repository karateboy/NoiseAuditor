package models

import akka.actor.{Actor, Props}
import com.github.nscala_time
import com.github.nscala_time.time
import com.github.nscala_time.time.Imports._
import models.ModelHelper._
import models.ReportRecord.Noise
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Filters.{equal, gte, lt}
import play.api.Logger

import java.util.Date
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, blocking}

object NoiseSecAuditor {
  def props(reportInfo: ReportInfo, reportInfoOp: ReportInfoOp, reportRecordOp: ReportRecordOp,
            reportTolerance: ReportTolerance, auditLogOp: AuditLogOp, taskName: String,
            mntNum: Int, terminalMap: Map[Int, String], start: DateTime, end: DateTime) =
    Props(classOf[NoiseSecAuditor], reportInfo, reportInfoOp, reportRecordOp,
      reportTolerance, auditLogOp, taskName,
      mntNum: Int, terminalMap, start, end)

  def getMinIterator(start: DateTime, end: DateTime): Iterator[nscala_time.time.Imports.DateTime] = new Iterator[DateTime] {
    private var current = start

    override def hasNext: Boolean = current < end

    override def next(): time.Imports.DateTime = {
      val ret = current
      current = current.plusMinutes(1)
      ret
    }
  }

  def getHourIterator(start: DateTime, end: DateTime): Iterator[nscala_time.time.Imports.DateTime] = new Iterator[DateTime] {
    private var current = start

    override def hasNext: Boolean = current < end

    override def next(): time.Imports.DateTime = {
      val ret = current
      current = current.plusHours(1)
      ret
    }
  }

  case class GetNoiseSecData(day: DateTime)

  case class GetNoiseEventData(dt: DateTime)

  case class GetNoiseHourData(day: DateTime)

  case class GetNoiseDayData(day: DateTime)

  case class AuditNoiseSecData(dt: DateTime, data: Seq[MinRecord])

  case class AuditNoiseEventData(dt: DateTime, data: Seq[EventRecord])

  case class AuditNoiseHourData(dt: DateTime, data: Seq[NoiseRecord])

  case class AuditNoiseDayData(dt: DateTime, data: Seq[NoiseRecord])

  case object NoiseSecAuditComplete

  case object StartAudit

  case object AuditFinished
}

case class VerifiedHourRecord(hour: DateTime, totalLeq: Double, eventLeq: Double, totalLdn: Double, eventLdn: Double,
                              numEvent: Int, duration: Int, backLeq: Double, backLdn: Double, eventSEL:Double)

class NoiseSecAuditor(reportInfo: ReportInfo, reportInfoOp: ReportInfoOp, reportRecordOp: ReportRecordOp,
                      reportTolerance: ReportTolerance, auditLogOp: AuditLogOp, taskName: String,
                      mntNum: Int, terminalMap: Map[Int, String], start: DateTime, end: DateTime) extends Actor {

  import NoiseSecAuditor._

  self ! StartAudit

  import scala.collection.mutable.Map

  def checkSecDataLoss(day: DateTime, dayMap: Map[Date, MinRecord]): Unit = {
    var logList = List.empty[LogEntry]
    val minList: Iterator[DateTime] = getMinIterator(day, day.plusDays(1))

    for (now <- minList) {
      if (!dayMap.contains(now.toDate)) {
        val msg = s"缺少資料"
        logList = logList :+ LogEntry(mntNum, now.toDate, AuditLog.DataTypeNoiseSec, msg)
      } else {
        val minData = dayMap(now.toDate)
        if (minData.records.length != 60) {
          val msg = s"缺少資料"
          logList = logList :+ LogEntry(mntNum, now.toDate, AuditLog.DataTypeNoiseSec, msg)
        }
      }
    }
    auditLogOp.appendLog(AuditLogID(reportInfo._id, mntNum), logList)
  }

  def checkEventData(day: DateTime, dayMinMap: Map[Date, MinRecord], events: Seq[EventRecord]): Unit = {
    var logList = List.empty[LogEntry]

    for (event <- events) {
      if (event.duration == 0) {
        val msg = s"DURATION_TIME 等於0"
        logList = logList :+ LogEntry(mntNum, event._id.time, AuditLog.DataTypeNoiseEvent, msg)
      }

      val eventStart = new DateTime(event._id.time)
      val eventSecRecordOpts: Seq[Option[SecRecord]] =
        for {i <- 0 to event.duration
             timestamp = eventStart.plusSeconds(i)
             mintuePart = timestamp.withSecondOfMinute(0)
             minRecord = dayMinMap(mintuePart.toDate)
             secNum = timestamp.getSecondOfMinute
             } yield {
          if (secNum < minRecord.records.length) {
            Some(minRecord.records(secNum))
          } else {
            val msg = s"缺少對應每秒資料無法稽核"
            logList = logList :+ LogEntry(mntNum, event._id.time, AuditLog.DataTypeNoiseEvent, msg)
            None
          }
        }
      if (!eventSecRecordOpts.contains(None)) {
        val eventSecSel: Seq[Double] = eventSecRecordOpts.flatten.map(_.value)
        val eventLeq = 10 * Math.log10(eventSecSel.map(v => Math.pow(10, v / 10)).sum) - Math.log10(event.duration)
        var msg1 = "原始資料:"
        var msg2 = "稽核資料:"
        if (event.eventLeq > eventLeq + reportTolerance.eventLeq || event.eventLeq < eventLeq - reportTolerance.eventLeq) {
          msg1 = msg1 + "EVENT_Leq=%.1f ".format(event.eventLeq)
          msg2 = msg2 + "EVENT_Leq=%.1f ".format(eventLeq)
        }
        val eventSel = eventLeq + 2 * Math.log10(event.duration)
        if (event.eventSel > eventSel + reportTolerance.eventLeq || event.eventSel < eventSel - reportTolerance.eventLeq) {
          msg1 = msg1 + "EVENT_SEL=%.1f".format(event.eventSel)
          msg2 = msg2 + "EVENT_SEL=%.1f".format(eventSel)
        }
        if (msg1 != "原始資料:")
          logList = logList :+ LogEntry(mntNum, event._id.time, AuditLog.DataTypeNoiseEvent, s"$msg1\n$msg2")
      }
    }

    auditLogOp.appendLog(AuditLogID(reportInfo._id, mntNum), logList)
  }

  def checkHourData(day: DateTime, hourMap: Map[Date, NoiseRecord], dayMinMap: Map[Date, MinRecord], eventMap: Map[Date, EventRecord]) = {
    val dayHours = getHourIterator(day, day.plusDays(1))

    def verifyHour(hour: DateTime, hourData: NoiseRecord): Option[VerifiedHourRecord] = {
      val msg1Header = "原始資料: "
      val msg2Header = "稽核資料: "
      var msg1 = msg1Header
      var msg2 = msg2Header

      val events: List[EventRecord] = eventMap.values.filter(evt => {
        val dt = new DateTime(evt._id.time)
        dt >= hour && dt < hour.plusHours(1)
      }).toList

      if (events.size == 0) {
        return None
      }

      def logIfWrong(v: Double, auditV: Double, error: Double, msgTag: String) = {
        if (v >= auditV + error || v < auditV - error) {
          msg1 = msg1 + msgTag.format(v)
          msg2 = msg2 + msgTag.format(auditV)
        }
      }

      val hourSecSELs: Seq[SecRecord] = {
        val minList: Iterator[DateTime] = getMinIterator(hour, hour.plusHours(1))
        val hourSeqSecSELs: Iterator[Seq[SecRecord]] =
          for (m <- minList) yield {
            if (dayMinMap.contains(m)) {
              val minRecord = dayMinMap(m)
              if (minRecord.records.size != 60)
                Logger.error(s"$mntNum $m has only ${minRecord.records.size} sec")

              minRecord.records
            } else {
              Logger.error(s"dayMinMap has no data $m")
              Seq.empty[SecRecord]
            }
          }

        val hourSecSelList = hourSeqSecSELs.toList
        hourSecSelList.flatten
      }

      if (hourSecSELs.size != hourData.activity) {
        val logs = Seq(LogEntry(mntNum, hour, AuditLog.DataTypeNoiseHour,
          s"原始資料: 小時噪音監測資料有效蒐集秒數${hourData.activity}"),
          LogEntry(mntNum, hour, AuditLog.DataTypeNoiseHour,
            s"稽核資料: 每秒噪音監測資料秒數${hourSecSELs.size}")
        )

        auditLogOp.appendLog(AuditLogID(reportInfo._id, mntNum), logs)
        return None
      }

      val totalLeq = 10 * Math.log10({
        val sum = hourSecSELs.map(sel => Math.pow(10, sel.value / 10)).sum
        sum / hourData.activity
      })

      val eventLeq = 10 * Math.log10(events.map(evt => Math.pow(10, evt.eventLeq / 10) * evt.duration).sum / hourData.activity)

      val eventSEL = eventLeq + 10 * Math.log10(hourData.activity)

      val backLeq = 10 * Math.log10(Math.pow(10, totalLeq / 10) - Math.pow(10, eventLeq / 10))

      val totalLdn = if (hour.getHourOfDay <= 7 || hour.getHourOfDay >= 22)
        totalLeq + 10
      else
        totalLeq

      val eventLdn = if (hour.getHourOfDay <= 7 || hour.getHourOfDay >= 22)
        eventLeq + 10
      else
        eventLeq

      val backLdn = if (hour.getHourOfDay <= 7 || hour.getHourOfDay >= 22)
        backLeq + 10
      else
        backLeq

      val numEvent = events.size

      val duration = events.map(_.duration).sum
      logIfWrong(hourData.backLdn, backLdn, reportTolerance.backLdn, "BACK_Ldn=%.1f ")
      logIfWrong(hourData.backLeq, backLeq, reportTolerance.backLeq, "BACK_Leq=%.1f ")
      logIfWrong(hourData.eventLeq, eventLeq, reportTolerance.eventLeq, "EVENT_Leq=%.1f ")
      logIfWrong(hourData.eventLdn, eventLdn, reportTolerance.eventLdn, "EVENT_Ldn=%.1f ")
      logIfWrong(hourData.totalLdn, totalLdn, reportTolerance.totalLdn, "TOTAL_Ldn=%.1f ")
      logIfWrong(hourData.numEvent, numEvent, reportTolerance.numOfEvent, "NUM_OF_EVENT=%.0f ")
      logIfWrong(hourData.totalLeq, totalLeq, reportTolerance.totalLeq, "TOTAL_Leq=%.1f ")
      logIfWrong(hourData.totalEvent, eventSEL, reportTolerance.totalEventSel, "TOTAL_EVENT_SEL=%.1f ")
      logIfWrong(hourData.duration, duration, reportTolerance.duration, "DURATION=%.0f")

      if (msg1 != msg1Header) {
        val logs = Seq(LogEntry(mntNum, hour, AuditLog.DataTypeNoiseHour, msg1),
          LogEntry(mntNum, hour, AuditLog.DataTypeNoiseHour, msg2))
        auditLogOp.appendLog(AuditLogID(reportInfo._id, mntNum), logs)
      }
      Some(VerifiedHourRecord(hour = hour, totalLeq = totalLeq, totalLdn = totalLdn, eventLeq = eventLeq,
        eventLdn = eventLdn, numEvent = numEvent, duration = duration, backLeq = backLeq, backLdn = backLdn,
        eventSEL = eventSEL))
    }

    val ret: Iterator[Option[VerifiedHourRecord]] =
      dayHours.map({
        current =>
          if (!hourMap.contains(current)) {
            val msg = s"缺少小時資料"
            auditLogOp.appendLog(AuditLogID(reportInfo._id, mntNum),
              Seq(LogEntry(mntNum, current, AuditLog.DataTypeNoiseHour, msg)))
            None
          } else {
            try {
              verifyHour(current, hourMap(current))
            } catch {
              case ex: Exception =>
                Logger.error(s"faile to verify hour ${current}", ex)
                None
            }
          }
      })
    ret.toList.flatten.map(v => v.hour -> v).toMap
  }

  def checkDayData(thisDay: DateTime, dayData: Seq[NoiseRecord], verifiedHourMap: Map[DateTime, VerifiedHourRecord]) {
    if (dayData.isEmpty) {
      val msg = s"缺少日資料"
      auditLogOp.appendLog(AuditLogID(reportInfo._id, mntNum),
        Seq(LogEntry(mntNum, thisDay, AuditLog.DataTypeNoiseDay, msg)))
      return
    }

    val data: NoiseRecord = dayData(0)
    val msg1Header = "原始資料: "
    val msg2Header = "稽核資料: "
    var msg1 = msg1Header
    var msg2 = msg2Header

    def logIfWrong(v: Double, auditV: Double, error: Double, msgTag: String) = {
      if (v >= auditV + error || v < auditV - error) {
        msg1 = msg1 + msgTag.format(v)
        msg2 = msg2 + msgTag.format(auditV)
      }
    }

    val verifiedHrList: List[VerifiedHourRecord] = verifiedHourMap.filter(
      p => p._1 >= thisDay && p._1 < thisDay.plusDays(1)).values.toList

    val hourCount = verifiedHrList.size
    val totalLeq = 10 * Math.log10(verifiedHrList.map(h => Math.pow(10, h.totalLeq / 10) * 3600).sum / (3600 * hourCount))
    val eventLeq = 10 * Math.log10(verifiedHrList.map(h => Math.pow(10, h.eventLeq / 10) * 3600).sum / (3600 * hourCount))
    val totalLdn = 10 * Math.log10(verifiedHrList.map(h => Math.pow(10, h.totalLdn / 10) * 3600).sum / (3600 * hourCount))
    val eventLdn = 10 * Math.log10(verifiedHrList.map(h => Math.pow(10, h.eventLdn / 10) * 3600).sum / (3600 * hourCount))
    val backLdn = 10 * Math.log10(verifiedHrList.map(h => Math.pow(10, h.backLdn / 10) * 3600).sum / (3600 * hourCount))
    val backLeq = 10 * Math.log10(verifiedHrList.map(h => Math.pow(10, h.backLeq / 10) * 3600).sum / (3600 * hourCount))
    val eventSEL = 10 * Math.log10(verifiedHrList.map(h => Math.pow(10, h.eventSEL / 10)).sum)
    val numEvent = verifiedHrList.map(_.numEvent).sum
    val duration = verifiedHrList.map(_.duration).sum

    logIfWrong(data.backLdn, backLdn, reportTolerance.backLdn, "BACK_Ldn=%.1f ")
    logIfWrong(data.backLeq, backLeq, reportTolerance.backLeq, "BACK_Leq=%.1f ")
    logIfWrong(data.eventLeq, eventLeq, reportTolerance.eventLeq, "EVENT_Leq=%.1f ")
    logIfWrong(data.eventLdn, eventLdn, reportTolerance.eventLdn, "EVENT_Ldn=%.1f ")
    logIfWrong(data.totalLdn, totalLdn, reportTolerance.totalLdn, "TOTAL_Ldn=%.1f ")
    logIfWrong(data.numEvent, numEvent, reportTolerance.numOfEvent, "NUM_OF_EVENT=%.0f ")
    logIfWrong(data.totalLeq, totalLeq, reportTolerance.totalLeq, "TOTAL_Leq=%.1f ")
    logIfWrong(data.totalEvent, eventSEL, reportTolerance.totalEventSel, "TOTAL_EVENT_SEL=%.1f ")
    logIfWrong(data.duration, duration, reportTolerance.duration, "DURATION=%.0f")

    if (msg1 != msg1Header) {
      val logs = Seq(LogEntry(mntNum, thisDay, AuditLog.DataTypeNoiseDay, msg1),
        LogEntry(mntNum, thisDay, AuditLog.DataTypeNoiseDay, msg2))
      auditLogOp.appendLog(AuditLogID(reportInfo._id, mntNum), logs)
    }
  }

  override def receive: Receive = auditStateMachine(Map.empty[Date, MinRecord], Map.empty[Date, EventRecord], Map.empty[DateTime, VerifiedHourRecord])

  def auditStateMachine(secDataMap: Map[Date, MinRecord], eventMap: Map[Date, EventRecord], verifiedHourMap: Map[DateTime, VerifiedHourRecord]): Receive = {
    case StartAudit =>
      val log = AuditLog(AuditLogID(reportInfo._id, mntNum), reportTolerance)
      auditLogOp.upsert(log)

      self ! GetNoiseSecData(start)

    case GetNoiseSecData(day) =>
      val filter = Filters.and(equal("_id.terminalID", mntNum),
        gte("_id.time", day.toDate), lt("_id.time", day.plusDays(1).toDate()))
      val f = reportRecordOp.getMinCollection(Noise).find(filter).toFuture()
      f onFailure errorHandler
      for (data <- f)
        self ! AuditNoiseSecData(day, data)

    case GetNoiseEventData(day) =>
      val filter = Filters.and(equal("_id.terminalID", mntNum),
        gte("_id.time", day.toDate), lt("_id.time", day.plusDays(1).toDate()))
      val f = reportRecordOp.getEventCollection.find(filter).toFuture()
      f onFailure errorHandler
      for (data <- f)
        self ! AuditNoiseEventData(day, data)

    case AuditNoiseSecData(dt, data) =>
      Future {
        blocking {
          val dataMap = Map.empty[Date, MinRecord]
          for (minRecord <- data)
            dataMap.update(minRecord._id.time, minRecord)

          checkSecDataLoss(dt, dataMap)

          context become auditStateMachine(dataMap, eventMap, verifiedHourMap)
          self ! GetNoiseEventData(dt)
        }
      }

    case AuditNoiseEventData(dt, data) =>
      val evtMap = Map.empty[Date, EventRecord]
      for (evt <- data)
        evtMap.update(evt._id.time, evt)


      context become auditStateMachine(secDataMap, evtMap, verifiedHourMap)
      checkEventData(dt, secDataMap, data)
      self ! GetNoiseHourData(dt)

    case GetNoiseHourData(day) =>
      val filter = Filters.and(equal("_id.terminalID", mntNum),
        gte("_id.time", day.toDate), lt("_id.time", day.plusDays(1).toDate()))
      val f = reportRecordOp.getNoiseCollection(ReportRecord.Hour).find(filter).toFuture()
      f onFailure errorHandler
      for (data <- f)
        self ! AuditNoiseHourData(day, data)

    case AuditNoiseHourData(thisDay, data) =>
      Future {
        blocking {
          val hourMap = Map.empty[Date, NoiseRecord]
          for (evt <- data)
            hourMap.update(evt._id.time, evt)

          val verifiedMap = checkHourData(thisDay, hourMap, secDataMap, eventMap)
          context become auditStateMachine(secDataMap, eventMap, verifiedHourMap ++ verifiedMap)
          self ! GetNoiseDayData(thisDay)

          if (thisDay.plusDays(1) < end)
            self ! GetNoiseSecData(thisDay.plusDays(1))
        }
      }
    case GetNoiseDayData(day) =>
      val filter = Filters.and(equal("_id.terminalID", mntNum),
        gte("_id.time", day.toDate), lt("_id.time", day.plusDays(1).toDate()))
      val f = reportRecordOp.getNoiseCollection(ReportRecord.Day).find(filter).toFuture()
      f onFailure errorHandler
      for (data <- f)
        self ! AuditNoiseDayData(day, data)

    case AuditNoiseDayData(thisDay, data) =>
      Future {
        blocking {
          try {
            checkDayData(thisDay, data, verifiedHourMap)
            reportInfoOp.incSubTaskCurrentCount(reportInfo._id, taskName)
          } catch {
            case ex: Throwable =>
              Logger.error(s"failed to audit day data", ex)
          }

          if (thisDay.plusDays(1) >= end) {
            reportInfoOp.removeSubTask(reportInfo._id, taskName)
            context.parent ! NoiseSecAuditComplete
          }
        }
      }
  }
}




