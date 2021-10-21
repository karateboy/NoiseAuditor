package models

import akka.actor._
import com.github.nscala_time.time.{DurationBuilder, StaticDuration}
import com.linuxense.javadbf.DBFUtils
import models.ModelHelper._
import models.ReportRecord.{Noise, RecordPeriod, WindDirection, WindSpeed}
import org.apache.commons.io.FileUtils
import org.mongodb.scala.MongoCollection
import play.api._

import java.io.{BufferedInputStream, File}
import java.nio.file.{Files, Path, Paths}
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, blocking}
import com.github.nscala_time.time.Imports._

object ReportImporter {
  var n = 0
  private var actorRefMap = Map.empty[String, ActorRef]

  def start(dataFile: File, airportInfoOp: AirportInfoOp, reportInfo: ReportInfo,
            reportInfoOp: ReportInfoOp, reportRecordOp: ReportRecordOp,
            reportTolerance: ReportTolerance, auditLogOp: AuditLogOp)(implicit actorSystem: ActorSystem) = {
    val name = getName
    val actorRef = actorSystem.actorOf(ReportImporter.props(dataFile = dataFile, airportInfoOp = airportInfoOp,
      reportInfo = reportInfo, reportInfoOp, reportRecordOp, reportTolerance = reportTolerance,
      auditLogOp = auditLogOp), name)
    actorRef ! ExtractZipFile
    actorRefMap = actorRefMap + (name -> actorRef)
    name
  }

  def getName = {
    n = n + 1
    s"dataImporter${n}"
  }

  def props(dataFile: File, airportInfoOp: AirportInfoOp, reportInfo: ReportInfo,
            reportInfoOp: ReportInfoOp, reportRecordOp: ReportRecordOp,
            reportTolerance: ReportTolerance, auditLogOp: AuditLogOp) =
    Props(classOf[ReportImporter], dataFile, airportInfoOp, reportInfo,
      reportInfoOp, reportRecordOp, reportTolerance, auditLogOp)

  def reaudit(airportInfoOp: AirportInfoOp, reportInfo: ReportInfo, reportInfoOp: ReportInfoOp,
              reportRecordOp: ReportRecordOp, reportTolerance: ReportTolerance, auditLogOp: AuditLogOp)
             (implicit actorSystem: ActorSystem) = {
    val name = getName
    val actorRef = actorSystem.actorOf(ReportImporter.props(dataFile = new File(""), airportInfoOp = airportInfoOp,
      reportInfo = reportInfo, reportInfoOp, reportRecordOp, reportTolerance = reportTolerance,
      auditLogOp = auditLogOp), name)
    actorRefMap = actorRefMap + (name -> actorRef)
    actorRef ! Reaudit
    name
  }

  def finish(actorName: String) = {
    actorRefMap = actorRefMap.filter(p => {
      p._1 != actorName
    })
  }

  def isFinished(actorName: String) = {
    !actorRefMap.contains(actorName)
  }

  def listAllDbfFile(path: String) = {

  }

  sealed trait FileType

  case class TaskAbort(reason: String)

  case class ImportSecFile(path: Path, task: SubTask)

  case object ImportDatabase

  case object ExtractZipFile

  case object ImportNoiseEvent

  case object ImportTestNoiseEvent

  case object ImportSecondNoise

  case object ImportSecondWind

  case object ImportHourlyNoise

  case object ImportHourlyWeather

  case object ImportDailyFlight

  case object ImportDailyNoise

  case object ImportMonthlyNoise

  case object ImportQuarterNoise

  case object ImportYearlyNoise

  case object TaskComplete

  case object AuditReport

  case object Reaudit

  case object AdutiEventData

  case object AuditHourData

  case object AuditDayData

  case object AuditMonthData

  case object AuditQuarterData
}

class ReportImporter(dataFile: File, airportInfoOp: AirportInfoOp,
                     reportInfo: ReportInfo, reportInfoOp: ReportInfoOp,
                     reportRecordOp: ReportRecordOp, reportTolerance: ReportTolerance, auditLogOp: AuditLogOp) extends Actor {

  import ReportImporter._

  val terminalMap: Map[Int, String] = waitReadyResult(getTerminalMap())

  override def receive: Receive = decompressPhase

  def decompressPhase(): Receive = {
    case ExtractZipFile =>
      Future {
        blocking {
          try {
            Logger.info(s"decompress ${dataFile.getAbsolutePath}")
            val parentPath = dataFile.getParent
            val cmd = """"C:/Program Files/7-Zip/7z.exe" x """ + dataFile.getAbsolutePath
            val process = Runtime.getRuntime.exec(cmd,
              Array.empty[String], new File(parentPath))
            reportInfoOp.updateState(reportInfo._id, "解壓縮中")
            process.waitFor()
            if (process.exitValue() != 0) {
              val message = "解壓縮失敗: 檔案錯誤"
              reportInfo.state = message
              reportInfo.appendUnableAuditReason("上傳檔案解壓縮失敗")
              reportInfoOp.upsertReportInfo(reportInfo)
              self ! TaskComplete
            } else {
              val message = "解壓縮成功"
              reportInfo.state = message
              dataFile.delete()

              context become importDbfPhase(parentPath, 0)
              finish(context.self.path.name)
              self ! ImportDatabase
            }
          } catch {
            case ex: Exception =>
              Logger.error("failed to import", ex)
              self ! TaskAbort("無法處理上傳檔案")
          }
        }
      }

    case Reaudit =>
      context become auditReportPhase(0)
      reportInfoOp.clearAllSubTasks(reportInfo._id)
      self ! AuditReport

    case TaskAbort(reason) =>
      reportInfo.state = reason
      reportInfo.appendUnableAuditReason(reason)
      reportInfoOp.upsertReportInfo(reportInfo)
      finish(context.self.path.name)
      self ! PoisonPill
  }

  def getDbfList(mainFolder: String, relativePath: String): List[Path] = {
    val paths: List[Path] = Files.list(Paths.get(mainFolder, relativePath)).iterator().asScala.toList
    val ret = paths.filter(!Files.isDirectory(_)).filter(p => p.toFile.getAbsolutePath.endsWith("dbf"))
    Logger.info(s"$mainFolder\\$relativePath #=${ret.size}")
    ret
  }

  def importSecData(mainFolder: String, relativePath: String) = {
    Future {
      blocking {
        val fileCount = getDbfList(mainFolder, relativePath)
        var count = 0
        val subTask = SubTask(s"匯入${relativePath}", count, fileCount.size)
        reportInfoOp.addSubTask(reportInfo._id, subTask)
        var dfeList = List.empty[DataFormatError]
        for (path <- fileCount) {
          val recordType =
            if (path.toFile.getName.contains("wd"))
              WindDirection
            else if (path.toFile.getName.contains("ws"))
              WindSpeed
            else
              Noise

          import com.linuxense.javadbf.DBFReader
          var reader: DBFReader = null
          var recordList = Seq.empty[MinRecord]
          var rowObjects: Array[Object] = null
          try {
            reader = new DBFReader(new BufferedInputStream(Files.newInputStream(path)))
            count = count + 1
            reportInfoOp.incSubTaskCurrentCount(reportInfo._id, subTask.name)

            do {
              rowObjects = reader.nextRecord()
              if (rowObjects != null) {
                try {
                  val mntNumber = rowObjects(0).asInstanceOf[String].trim.toInt
                  val mntName = rowObjects(1).asInstanceOf[String].trim

                  val startDate = try {
                    LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy-MM-dd"))
                  } catch {
                    case _: IllegalArgumentException =>
                      LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy/MM/dd"))
                  }
                  val startTime = LocalTime.parse(rowObjects(3).asInstanceOf[String].trim, DateTimeFormat.forPattern("HH:mm"))
                  val start = startDate.toLocalDateTime(startTime)
                  var secRecords = Seq.empty[SecRecord]
                  for (i <- 0 to 59) {
                    try {
                      secRecords = secRecords :+ SecRecord(start.plusSeconds(i).toDate(), rowObjects(i + 5).asInstanceOf[java.math.BigDecimal].doubleValue())
                    } catch {
                      case _: Throwable =>
                        val dfe = DataFormatError(fileName = path.toFile.getName, terminal = mntName, time = s"${start.plusSeconds(i).toString("yyyy/MM/dd HH:mm:ss")}",
                          dataType = "每秒噪音監測資料",
                          fieldName = s"SL$i", errorInfo = "無資料", value = "")
                        dfeList = dfeList :+ (dfe)
                    }
                  }
                  val record = MinRecord(RecordID(start.toDate, mntNumber, recordType.toString), secRecords)
                  recordList = recordList :+ (record)
                } catch {
                  case ex: Exception =>
                    val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "", dataType = "",
                      fieldName = "", errorInfo = "檔案錯誤", value = "")
                    dfeList = dfeList :+ (dfe)
                }
              }
            } while (rowObjects != null)
            if (recordList.nonEmpty) {
              val collection = reportRecordOp.getMinCollection(recordType)
              val f = collection.insertMany(recordList).toFuture()
              f onFailure (errorHandler)
            } else {
              val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "", dataType = "",
                fieldName = "", errorInfo = "無資料", value = "")
              dfeList = dfeList :+ (dfe)
            }
          } catch {
            case ex: Exception =>
              val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "", dataType = "",
                fieldName = "", errorInfo = "無法讀取", value = "")
              dfeList = dfeList :+ (dfe)
              Logger.error(s"無法匯入 ${path.toFile.getPath}", ex)
          } finally {
            DBFUtils.close(reader)
          }
        }
        reportInfoOp.appendDataFormatErrors(reportInfo._id, dfeList)
        self ! TaskComplete
      }
    } onFailure ({
      case ex: Exception =>
        Logger.error(s"failed to import ${relativePath}", ex)
        self ! TaskAbort(s"無法匯入${relativePath}")
    })
  }

  import ReportRecord._

  def importNoiseData(mainFolder: String, relativePath: String, recordPeriod: RecordPeriod) = {
    Future {
      blocking {
        val fileCount = getDbfList(mainFolder, relativePath)
        var count = 0
        val subTask = SubTask(s"匯入${relativePath}", count, fileCount.size)
        reportInfoOp.addSubTask(reportInfo._id, subTask)
        var dfeList = List.empty[DataFormatError]
        for (path <- fileCount) {
          import com.linuxense.javadbf.DBFReader
          var reader: DBFReader = null
          var recordList = Seq.empty[NoiseRecord]
          var rowObjects: Array[Object] = null
          try {
            reader = new DBFReader(new BufferedInputStream(Files.newInputStream(path)))
            count = count + 1
            reportInfoOp.incSubTaskCurrentCount(reportInfo._id, subTask.name)

            do {
              rowObjects = reader.nextRecord()
              if (rowObjects != null) {
                try {
                  val mntNumber = rowObjects(0).asInstanceOf[String].trim.toInt
                  val mntName = rowObjects(1).asInstanceOf[String].trim
                  val startDate = try {
                    LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy-MM-dd"))
                  } catch {
                    case _: IllegalArgumentException =>
                      LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy/MM/dd"))
                  }
                  val startTime = LocalTime.parse(rowObjects(3).asInstanceOf[String].trim, DateTimeFormat.forPattern("HH:mm:ss"))
                  val start = startDate.toLocalDateTime(startTime)
                  val activity = rowObjects(4).asInstanceOf[java.math.BigDecimal].toBigInteger.intValue()
                  val totalEvent = rowObjects(5).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val totalLeq = rowObjects(6).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val eventLeq = rowObjects(7).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val backLeq = rowObjects(8).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val totalLdn = rowObjects(9).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val eventLdn = rowObjects(10).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val backLdn = rowObjects(11).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val l5 = rowObjects(12).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val l10 = rowObjects(13).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val l50 = rowObjects(14).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val l90 = rowObjects(15).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val l95 = rowObjects(16).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val l99 = rowObjects(17).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val numEvent = rowObjects(18).asInstanceOf[java.math.BigDecimal].toBigInteger.intValue()
                  val duration = rowObjects(19).asInstanceOf[java.math.BigDecimal].toBigInteger.intValue()
                  val record = NoiseRecord(RecordID(start.toDate, mntNumber, Noise.toString), activity = activity,
                    totalEvent = totalEvent,
                    totalLeq = totalLeq, eventLeq = eventLeq, backLeq = backLeq, totalLdn = totalLdn,
                    eventLdn = eventLdn, backLdn = backLdn,
                    l5 = l5, l10 = l10, l50 = l50, l90 = l90, l95 = l95, l99 = l99, numEvent = numEvent, duration = duration)
                  recordList = recordList :+ (record)
                } catch {
                  case ex: Exception =>
                    val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "",
                      dataType = s"$relativePath",
                      fieldName = "", errorInfo = "資料格式錯誤", value = ex.getMessage)
                    dfeList = dfeList :+ (dfe)
                    Logger.error(s"${path.toFile.getPath} 忽略第${count}筆錯誤資料", ex)
                }
              }
            } while (rowObjects != null)
            if (recordList.nonEmpty) {
              val collection = reportRecordOp.getNoiseCollection(recordPeriod)
              val f = collection.insertMany(recordList).toFuture()
              f onFailure (errorHandler)
            } else {
              val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "",
                dataType = s"$relativePath",
                fieldName = "", errorInfo = "無資料", value = "")
              dfeList = dfeList :+ (dfe)
            }
          } catch {
            case ex: Exception =>
              val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "",
                dataType = s"$relativePath",
                fieldName = "", errorInfo = "無法讀取資料", value = "")
              dfeList = dfeList :+ (dfe)
              Logger.error(s"無法匯入 ${path.toFile.getPath}", ex)
          } finally {
            DBFUtils.close(reader)
          }
        }
        reportInfoOp.appendDataFormatErrors(reportInfo._id, dfeList)
        self ! TaskComplete
      }
    } onFailure ({
      case ex: Exception =>
        Logger.error(s"failed to import ${relativePath}", ex)
        self ! TaskAbort(s"無法匯入${relativePath}")
    })
  }

  def importWindHourData(mainFolder: String, relativePath: String) = {
    Future {
      blocking {
        val fileCount = getDbfList(mainFolder, relativePath)
        var count = 0
        val subTask = SubTask(s"匯入${relativePath}", count, fileCount.size)
        reportInfoOp.addSubTask(reportInfo._id, subTask)
        var dfeList = List.empty[DataFormatError]
        for (path <- fileCount) {
          import com.linuxense.javadbf.DBFReader
          var reader: DBFReader = null
          var recordList = Seq.empty[WindHourRecord]
          var rowObjects: Array[Object] = null
          try {
            reader = new DBFReader(new BufferedInputStream(Files.newInputStream(path)))
            count = count + 1
            reportInfoOp.incSubTaskCurrentCount(reportInfo._id, subTask.name)

            do {
              rowObjects = reader.nextRecord()
              if (rowObjects != null) {
                try {
                  val mntNumber = rowObjects(0).asInstanceOf[String].trim.toInt
                  val mntName = rowObjects(1).asInstanceOf[String].trim
                  val startDate = try {
                    LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy-MM-dd"))
                  } catch {
                    case _: IllegalArgumentException =>
                      LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy/MM/dd"))
                  }
                  val startTime = LocalTime.parse(rowObjects(3).asInstanceOf[String].trim, DateTimeFormat.forPattern("HH:mm:ss"))
                  val start = startDate.toLocalDateTime(startTime)
                  val windSpeedAvg = rowObjects(5).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val windSpeedMax = rowObjects(6).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val record = WindHourRecord(RecordID(start.toDate, mntNumber, WindSpeed.toString),
                    windSpeedAvg, windSpeedMax)
                  recordList = recordList :+ (record)
                } catch {
                  case ex: Exception =>
                    Logger.error(s"${path.toFile.getPath} 忽略第${count}筆錯誤資料", ex)
                    val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "",
                      dataType = s"$relativePath",
                      fieldName = "", errorInfo = "檔案錯誤", value = ex.getMessage)
                    dfeList = dfeList :+ (dfe)
                }
              }
            } while (rowObjects != null)
            if (recordList.nonEmpty) {
              val collection = reportRecordOp.getWindHourCollection
              val f = collection.insertMany(recordList).toFuture()
              f onFailure (errorHandler)
            } else {
              val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "", dataType = "",
                fieldName = "", errorInfo = "無資料", value = "")
              dfeList = dfeList :+ (dfe)
            }
          } catch {
            case ex: Exception =>
              Logger.error(s"無法匯入 ${path.toFile.getPath}", ex)
              val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "", dataType = "",
                fieldName = "", errorInfo = "無法讀取", value = "")
              dfeList = dfeList :+ (dfe)
          } finally {
            DBFUtils.close(reader)
          }
        }
        reportInfoOp.appendDataFormatErrors(reportInfo._id, dfeList)
        self ! TaskComplete
      }
    } onFailure ({
      case ex: Exception =>
        Logger.error(s"failed to import ${relativePath}", ex)
        // Ignore Wind data
        self ! TaskAbort(s"無法匯入${relativePath}")
    })
  }

  def importEventData(mainFolder: String, relativePath: String, collection: MongoCollection[EventRecord]) = {
    Future {
      blocking {
        val fileCount = getDbfList(mainFolder, relativePath)
        var count = 0
        val subTask = SubTask(s"匯入${relativePath}", count, fileCount.size)
        reportInfoOp.addSubTask(reportInfo._id, subTask)
        var dfeList = List.empty[DataFormatError]
        for (path <- fileCount) {
          import com.linuxense.javadbf.DBFReader
          var reader: DBFReader = null
          var rowObjects: Array[Object] = null
          var terminal = ""
          var time = ""
          var fieldName = ""
          var reason = ""
          try {
            reader = new DBFReader(new BufferedInputStream(Files.newInputStream(path)))
            count = count + 1
            reportInfoOp.incSubTaskCurrentCount(reportInfo._id, subTask.name)
            var row = 0
            do {
              rowObjects = reader.nextRecord()
              row = row + 1
              terminal = ""
              time = ""
              fieldName = ""
              reason = ""
              if (rowObjects != null) {
                try {
                  val mntNumber = try {
                    rowObjects(0).asInstanceOf[String].trim.toInt
                  }catch {
                    case ex:Throwable =>
                      fieldName = "MNT_NUMBER"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }
                  val mntName = rowObjects(1).asInstanceOf[String].trim
                  val startDate: LocalDate = try {
                    LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy-MM-dd"))
                  } catch {
                    case _: IllegalArgumentException =>
                      try{
                        LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy/MM/dd"))

                      }catch{
                        case ex:Throwable=>
                          fieldName = "START_DATE"
                          reason = s"第${row}行格式錯誤"
                          throw ex
                      }
                  }
                  val startTime = try {
                    LocalTime.parse(rowObjects(3).asInstanceOf[String].trim, DateTimeFormat.forPattern("HH:mm:ss"))
                  }catch{
                    case ex:Throwable=>
                      fieldName = "START_TIME"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }
                  val start = startDate.toLocalDateTime(startTime)
                  time = start.toString("yyyy/MM/dd HH:mm:ss")
                  val duration = try {
                    rowObjects(4).asInstanceOf[java.math.BigDecimal].toBigInteger.intValue()
                  }catch {
                    case ex:Throwable=>
                      fieldName = "DURATION_T"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }
                  val setl = try{
                    rowObjects(5).asInstanceOf[java.math.BigDecimal].doubleValue()
                  }catch {
                    case ex:Throwable=>
                      fieldName = "SETL"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }
                  val minDurationTime = try{
                    rowObjects(6).asInstanceOf[java.math.BigDecimal].toBigInteger.intValue()
                  }catch {
                    case ex:Throwable=>
                      fieldName = "MIN_DUR_TIME"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }
                  val eventLeq = try{
                    rowObjects(7).asInstanceOf[java.math.BigDecimal].doubleValue()
                  }catch {
                    case ex:Throwable=>
                      fieldName = "EVENT_Leq"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }
                  val eventSel = try{
                    rowObjects(8).asInstanceOf[java.math.BigDecimal].doubleValue()
                  }catch {
                    case ex:Throwable=>
                      fieldName = "EVENT_SEL"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }
                  val eventMaxLen = try{
                    rowObjects(9).asInstanceOf[java.math.BigDecimal].doubleValue()
                  }catch {
                    case ex:Throwable=>
                      fieldName = "EVENT_MAX_LEVEL"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }
                  val eventMaxT = try {
                    LocalTime.parse(rowObjects(10).asInstanceOf[String].trim, DateTimeFormat.forPattern("HH:mm:ss"))
                  }catch {
                    case ex:Throwable=>
                      fieldName = "EVENT_MAX_TIME"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }
                  var secRecords = Seq.empty[SecRecord]
                  for (i <- 0 to 122) {
                    try {
                      val fielfName = s"SL$i"
                      rowObjects()
                      secRecords = secRecords :+ SecRecord(start.plusSeconds(i).toDate(), rowObjects(i + 11).asInstanceOf[java.math.BigDecimal].doubleValue())
                    } catch {
                      case _: Exception =>
                    }
                  }
                  val record = EventRecord(RecordID(start.toDate, mntNumber, Event.toString), duration = duration,
                    setl = setl, minDur = minDurationTime, eventLeq = eventLeq, eventSel = eventSel, eventMaxLen = eventMaxLen,
                    eventMaxTime = startDate.toLocalDateTime(eventMaxT).toDate(), secRecords)
                  val f = collection.insertOne(record).toFuture()
                  f onFailure({
                    case ex:Throwable=>
                      val recordTerminal = terminalMap(record._id.terminalID)
                      val time = new DateTime(record._id.time).toString("YYYY/MM/dd HH:mm:ss")
                      val dfe = DataFormatError(fileName = path.toFile.getName, terminal = recordTerminal,
                        time = time,
                        dataType = s"$relativePath",
                        fieldName = fieldName, errorInfo = "資料重複", value = ex.getMessage)
                      dfeList = dfeList :+ (dfe)
                  })
                } catch {
                  case ex: Exception =>
                    val dfe = DataFormatError(fileName = path.toFile.getName, terminal = terminal, time = time,
                      dataType = s"$relativePath",
                      fieldName = fieldName, errorInfo = reason, value = ex.getMessage)
                    dfeList = dfeList :+ (dfe)
                }
              }
            } while (rowObjects != null)
          } catch {
            case ex: Exception =>
              val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "",
                dataType = s"$relativePath",
                fieldName = "", errorInfo = "無法讀取檔案", value = "")
              dfeList = dfeList :+ (dfe)
              Logger.error(s"無法匯入 ${path.toFile.getPath}", ex)
          } finally {
            DBFUtils.close(reader)
          }
        }
        reportInfoOp.appendDataFormatErrors(reportInfo._id, dfeList)
        self ! TaskComplete
      }
    } onFailure ({
      case ex: Exception =>
        Logger.error(s"failed to import ${relativePath}", ex)
        self ! TaskAbort(s"無法匯入${relativePath}")
    })
  }

  def importFlightData(mainFolder: String, relativePath: String) = {
    Future {
      blocking {
        val fileCount = getDbfList(mainFolder, relativePath)
        var count = 0
        val subTask = SubTask(s"匯入${relativePath}", count, fileCount.size)
        reportInfoOp.addSubTask(reportInfo._id, subTask)
        var dfeList = List.empty[DataFormatError]
        for (path <- fileCount) {
          import com.linuxense.javadbf.DBFReader
          var reader: DBFReader = null
          var rowObjects: Array[Object] = null
          try {
            reader = new DBFReader(new BufferedInputStream(Files.newInputStream(path)))
            count = count + 1
            reportInfoOp.incSubTaskCurrentCount(reportInfo._id, subTask.name)
            var terminal = ""
            var time = ""
            var fieldName = ""
            var reason = ""
            var row = 0
            do {
              rowObjects = reader.nextRecord()
              row = row + 1
              if (rowObjects != null) {
                try {
                  val startDate: LocalDate = try {
                    LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy-MM-dd"))
                  } catch {
                    case _: IllegalArgumentException =>
                      try{
                        LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy/MM/dd"))

                      }catch{
                        case ex:Throwable=>
                          fieldName = "START_DATE"
                          reason = s"第${row}行格式錯誤"
                          throw ex
                      }
                  }
                  val startTime = try {
                    LocalTime.parse(rowObjects(3).asInstanceOf[String].trim, DateTimeFormat.forPattern("HH:mm:ss"))
                  }catch{
                    case ex:Throwable=>
                      fieldName = "START_TIME"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }
                  val start = startDate.toLocalDateTime(startTime)
                  time = start.toString("yyyy/MM/dd HH:mm:ss")
                  val acftID = try{
                    rowObjects(2).asInstanceOf[String]
                  }catch{
                    case ex:Throwable=>
                      fieldName = "ACFT_ID"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }
                  val operation = try{
                    rowObjects(3).asInstanceOf[String]
                  }catch{
                    case ex:Throwable=>
                      fieldName = "OPERATION"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }
                  val runway = try{
                    rowObjects(4).asInstanceOf[String]
                  }catch{
                    case ex:Throwable=>
                      fieldName = "RUNWAY"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }
                  val flightRoute = try{
                    rowObjects(5).asInstanceOf[String]
                  }catch{
                    case ex:Throwable=>
                      fieldName = "FLIGHT_ROUTE"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }

                  val record = FlightInfo(start.toDate, acftID: String, operation: String, runway: String, flightRoute: String)
                  val collection = reportRecordOp.getFlightCollection
                  val f = collection.insertOne(record).toFuture()
                  f onFailure ({
                    case ex:Throwable=>
                      val time = new DateTime(record._id).toString("YYYY/MM/dd HH:mm:ss")
                      val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "",
                        time = time,
                        dataType = s"$relativePath",
                        fieldName = fieldName, errorInfo = "資料重複", value = ex.getMessage)
                      dfeList = dfeList :+ (dfe)
                  })
                } catch {
                  case ex: Exception =>
                    val dfe = DataFormatError(fileName = path.toFile.getName, terminal = terminal, time = time,
                      dataType = s"$relativePath",
                      fieldName = fieldName, errorInfo = reason, value = ex.getMessage)
                    dfeList = dfeList :+ (dfe)
                    Logger.error(s"${path.toFile.getPath} 忽略第${count}筆錯誤資料", ex)
                }
              }
            } while (rowObjects != null)
          } catch {
            case ex: Exception =>
              val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "",
                dataType = s"$relativePath",
                fieldName = "", errorInfo = "檔案無法讀取", value = "")
              dfeList = dfeList :+ (dfe)
              Logger.error(s"無法匯入 ${path.toFile.getPath}", ex)
          } finally {
            DBFUtils.close(reader)
          }
        }
        reportInfoOp.appendDataFormatErrors(reportInfo._id, dfeList)
        self ! TaskComplete
      }
    } onFailure ({
      case ex: Exception =>
        Logger.error(s"failed to import ${relativePath}", ex)
        self ! TaskComplete
    })
  }

  def getTerminalMap(): Future[Map[Int, String]] = {
    val infoID = reportInfo._id.airpotInfoID
    val f = airportInfoOp.get(infoID.airportID, infoID.year, infoID.quarter)
    for (ret <- f) yield {
      val pairs = ret(0).terminals.map(t => t.no -> t.name)
      pairs.toMap
    }
  }

  def importDbfPhase(mainFolder: String, importTasks: Int): Receive = {

    case ImportDatabase =>
      reportInfoOp.updateState(reportInfo._id, "匯入資料庫")
      self ! ImportSecondWind
      self ! ImportSecondNoise
      self ! ImportHourlyNoise
      self ! ImportHourlyWeather
      self ! ImportDailyNoise
      self ! ImportMonthlyNoise
      self ! ImportQuarterNoise
      self ! ImportYearlyNoise
      self ! ImportTestNoiseEvent
      self ! ImportNoiseEvent
      // self ! ImportDailyFlight

    case ImportSecondWind =>
      context become importDbfPhase(mainFolder, importTasks + 1)
      importSecData(mainFolder, "每秒風速監測資料")

    case ImportSecondNoise =>
      context become importDbfPhase(mainFolder, importTasks + 1)
      importSecData(mainFolder, "每秒噪音監測資料")

    case ImportHourlyNoise =>
      context become importDbfPhase(mainFolder, importTasks + 1)
      importNoiseData(mainFolder, "每小時噪音監測資料", ReportRecord.Hour)

    case ImportDailyNoise =>
      context become importDbfPhase(mainFolder, importTasks + 1)
      importNoiseData(mainFolder, "每日噪音監測資料", ReportRecord.Day)

    case ImportMonthlyNoise =>
      context become importDbfPhase(mainFolder, importTasks + 1)
      importNoiseData(mainFolder, "每月噪音監測資料", ReportRecord.Month)

    case ImportQuarterNoise =>
      context become importDbfPhase(mainFolder, importTasks + 1)
      importNoiseData(mainFolder, "每季噪音監測資料", ReportRecord.Quarter)

    case ImportYearlyNoise =>
      context become importDbfPhase(mainFolder, importTasks + 1)
      importNoiseData(mainFolder, "一年噪音監測資料", ReportRecord.Year)

    case ImportHourlyWeather =>
      context become importDbfPhase(mainFolder, importTasks + 1)
      importWindHourData(mainFolder, "每小時氣象監測資料")

    case ImportNoiseEvent =>
      context become importDbfPhase(mainFolder, importTasks + 1)
      importEventData(mainFolder, "噪音事件監測資料", reportRecordOp.getEventCollection)

    case ImportTestNoiseEvent =>
      context become importDbfPhase(mainFolder, importTasks + 1)
      importEventData(mainFolder, "試車噪音監測資料", reportRecordOp.getTestEventCollection)

    case ImportDailyFlight =>
      context become importDbfPhase(mainFolder, importTasks + 1)
      importFlightData(mainFolder, "飛航動態資料")

    case TaskComplete =>
      if (importTasks - 1 != 0)
        context become importDbfPhase(mainFolder, importTasks - 1)
      else {
        context become auditReportPhase(0)
        reportInfoOp.clearAllSubTasks(reportInfo._id)
        FileUtils.deleteDirectory(new File(mainFolder))
        self ! AuditReport
      }
    case TaskAbort(reason) =>
      reportInfo.state = reason
      reportInfo.appendUnableAuditReason(reason)
      reportInfoOp.upsertReportInfo(reportInfo)
      finish(context.self.path.name)
      self ! PoisonPill
  }

  def auditReportPhase(auditors: Int): Receive = {
    case AuditReport =>
      reportInfoOp.updateState(reportInfo._id, "產出稽核報告中")
      val start = new DateTime(reportInfo.year + 1911,
        (reportInfo.quarter - 1) * 3 + 1, 1, 0, 0)
      val end = start.plusMonths(3)
      val d : Duration = new Duration(start, end)
      Logger.info(s"total ${terminalMap.size} ${d.getStandardDays}")
      for (mntNum <- terminalMap.keys) {
        val subTask = SubTask(s"稽核${terminalMap(mntNum)}噪音資料", 0, d.getStandardDays.toInt)
        reportInfoOp.addSubTask(reportInfo._id, subTask)
        val props = NoiseSecAuditor.props(reportInfo, reportInfoOp, reportRecordOp,
          reportTolerance, auditLogOp, subTask.name,
          mntNum, terminalMap, start, end)
        context.actorOf(props)
      }
      context become auditReportPhase(auditors + terminalMap.keys.size)

    case NoiseSecAuditor.NoiseSecAuditComplete =>
      sender() ! PoisonPill
      self ! TaskComplete

    case TaskComplete =>
      if (auditors - 1 > 0)
        context become auditReportPhase(auditors - 1)
      else {
        reportInfoOp.updateState(reportInfo._id, "稽核完成")
        reportInfoOp.clearAllSubTasks(reportInfo._id)
        finish(context.self.path.name)
        self ! PoisonPill
      }
    case TaskAbort(reason) =>
      reportInfo.state = "失敗"
      reportInfo.appendUnableAuditReason(reason)
      reportInfoOp.upsertReportInfo(reportInfo)
      finish(context.self.path.name)
      self ! PoisonPill
  }
}
