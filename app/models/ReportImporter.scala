package models

import akka.actor._
import com.linuxense.javadbf.DBFUtils
import models.ModelHelper._
import models.ReportRecord.{Noise, RecordPeriod, WindDirection, WindSpeed}
import org.apache.commons.io.FileUtils
import org.joda.time.format.DateTimeFormat
import org.joda.time.{LocalDate, LocalTime}
import org.mongodb.scala.MongoCollection
import play.api._

import java.io.{BufferedInputStream, File}
import java.nio.file.{Files, Path, Paths}
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, blocking}

object ReportImporter {
  var n = 0
  private var actorRefMap = Map.empty[String, ActorRef]

  def start(dataFile: File, airportInfoOp: AirportInfoOp, reportInfo: ReportInfo, reportInfoOp: ReportInfoOp, reportRecordOp: ReportRecordOp)(implicit actorSystem: ActorSystem) = {
    val name = getName
    val actorRef = actorSystem.actorOf(ReportImporter.props(dataFile = dataFile, airportInfoOp = airportInfoOp,
      reportInfo = reportInfo, reportInfoOp, reportRecordOp), name)
    actorRefMap = actorRefMap + (name -> actorRef)
    name
  }

  def getName = {
    n = n + 1
    s"dataImporter${n}"
  }

  def props(dataFile: File, airportInfoOp: AirportInfoOp, reportInfo: ReportInfo, reportInfoOp: ReportInfoOp, reportRecordOp: ReportRecordOp) =
    Props(classOf[ReportImporter], dataFile, airportInfoOp, reportInfo, reportInfoOp, reportRecordOp)

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
}

class ReportImporter(dataFile: File, airportInfoOp: AirportInfoOp,
                     reportInfo: ReportInfo, reportInfoOp: ReportInfoOp, reportRecordOp: ReportRecordOp) extends Actor {

  import ReportImporter._

  self ! ExtractZipFile

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
              val f = getTerminalMap()
              for(map<-f){
                context become importDbfPhase(parentPath, 0, map)
                finish(context.self.path.name)
                self ! ImportDatabase
              }
            }
          } catch {
            case ex: Exception =>
              Logger.error("failed to import", ex)
              self ! TaskAbort("無法處理上傳檔案")
          }
        }
      }

    case TaskAbort(reason) =>
      reportInfo.state = "失敗"
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
            subTask.current = count
            reportInfoOp.incSubTaskCurrentCount(reportInfo._id, subTask)

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
                      case _:Throwable =>
                        val dfe = DataFormatError(fileName = path.toFile.getName, terminal = mntName, time = s"${start.plusSeconds(i).toString("yyyy/MM/dd HH:mm:ss")}",
                          dataType = "每秒噪音監測資料",
                          fieldName = s"SL$i", errorInfo = "無資料", value = "")
                        dfeList = dfeList:+(dfe)
                    }
                  }
                  val record = MinRecord(RecordID(start.toDate, mntNumber, recordType.toString), secRecords)
                  recordList = recordList :+ (record)
                } catch {
                  case ex: Exception =>
                    val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "", dataType = "",
                      fieldName = "", errorInfo = "檔案錯誤", value = "")
                    dfeList = dfeList:+(dfe)
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
              dfeList = dfeList:+(dfe)
            }
          } catch {
            case ex: Exception =>
              val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "", dataType = "",
                fieldName = "", errorInfo = "無法讀取", value = "")
              dfeList = dfeList:+(dfe)
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
            subTask.current = count
            reportInfoOp.incSubTaskCurrentCount(reportInfo._id, subTask)

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
                    dfeList = dfeList:+(dfe)
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
              dfeList = dfeList:+(dfe)
            }
          } catch {
            case ex: Exception =>
              val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "",
                dataType = s"$relativePath",
                fieldName = "", errorInfo = "無法讀取資料", value = "")
              dfeList = dfeList:+(dfe)
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
            subTask.current = count
            reportInfoOp.incSubTaskCurrentCount(reportInfo._id, subTask)

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
                    dfeList = dfeList:+(dfe)
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
              dfeList = dfeList:+(dfe)
            }
          } catch {
            case ex: Exception =>
              Logger.error(s"無法匯入 ${path.toFile.getPath}", ex)
              val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "", dataType = "",
                fieldName = "", errorInfo = "無法讀取", value = "")
              dfeList = dfeList:+(dfe)
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
          var recordList = Seq.empty[EventRecord]
          var rowObjects: Array[Object] = null
          try {
            reader = new DBFReader(new BufferedInputStream(Files.newInputStream(path)))
            count = count + 1
            subTask.current = count
            reportInfoOp.incSubTaskCurrentCount(reportInfo._id, subTask)

            do {
              rowObjects = reader.nextRecord()
              if (rowObjects != null) {
                try {
                  val mntNumber = rowObjects(0).asInstanceOf[String].trim.toInt
                  val mntName = rowObjects(1).asInstanceOf[String].trim
                  val startDate: LocalDate = try {
                    LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy-MM-dd"))
                  } catch {
                    case _: IllegalArgumentException =>
                      LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy/MM/dd"))
                  }
                  val startTime = LocalTime.parse(rowObjects(3).asInstanceOf[String].trim, DateTimeFormat.forPattern("HH:mm:ss"))
                  val start = startDate.toLocalDateTime(startTime)
                  val duration = rowObjects(4).asInstanceOf[java.math.BigDecimal].toBigInteger.intValue()
                  val setl = rowObjects(5).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val minDurationTime = rowObjects(6).asInstanceOf[java.math.BigDecimal].toBigInteger.intValue()
                  val eventLeq = rowObjects(7).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val eventSel = rowObjects(8).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val eventMaxLen = rowObjects(9).asInstanceOf[java.math.BigDecimal].doubleValue()
                  val eventMaxT = LocalTime.parse(rowObjects(10).asInstanceOf[String].trim, DateTimeFormat.forPattern("HH:mm:ss"))
                  var secRecords = Seq.empty[SecRecord]
                  for (i <- 0 to 120) {
                    try {
                      secRecords = secRecords :+ SecRecord(start.plusSeconds(i).toDate(), rowObjects(i + 11).asInstanceOf[java.math.BigDecimal].doubleValue())
                    } catch {
                      case ex: Exception =>
                        Logger.error(s"${path.toFile.getPath} ${mntName} ${start} 遺失第${i}秒資料")
                    }
                  }
                  val record = EventRecord(RecordID(start.toDate, mntNumber, Event.toString), duration = duration,
                    setl = setl, minDur = minDurationTime, eventLeq = eventLeq, eventSel = eventSel, eventMaxLen = eventMaxLen,
                    eventMaxTime = startDate.toLocalDateTime(eventMaxT).toDate(), secRecords)
                  recordList = recordList :+ (record)
                } catch {
                  case ex: Exception =>
                    val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "",
                      dataType = s"$relativePath",
                      fieldName = "", errorInfo = "格式錯誤", value = ex.getMessage)
                    dfeList = dfeList:+(dfe)
                    Logger.error(s"${path.toFile.getPath} 忽略第${count}筆錯誤資料", ex)
                }
              }
            } while (rowObjects != null)
            if (recordList.nonEmpty) {
              val f = collection.insertMany(recordList).toFuture()
              f onFailure({
                case _:Throwable=>
                  val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "",
                    dataType = s"$relativePath",
                    fieldName = "", errorInfo = "出現重複事件", value = "")
                  dfeList = dfeList:+(dfe)
              })
            } else {
              val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "",
                dataType = s"$relativePath",
                fieldName = "", errorInfo = "檔案無資料", value = "")
              dfeList = dfeList:+(dfe)
            }
          } catch {
            case ex: Exception =>
              val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "",
                dataType = s"$relativePath",
                fieldName = "", errorInfo = "無法讀取檔案", value = "")
              dfeList = dfeList:+(dfe)
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
          var recordList = Seq.empty[FlightInfo]
          var rowObjects: Array[Object] = null
          try {
            reader = new DBFReader(new BufferedInputStream(Files.newInputStream(path)))
            count = count + 1
            subTask.current = count
            reportInfoOp.incSubTaskCurrentCount(reportInfo._id, subTask)

            do {
              rowObjects = reader.nextRecord()
              if (rowObjects != null) {
                try {
                  val startDate = try {
                    LocalDate.parse(rowObjects(0).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy-MM-dd"))
                  } catch {
                    case _: IllegalArgumentException =>
                      LocalDate.parse(rowObjects(0).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy/MM/dd"))
                  }
                  val startTime = LocalTime.parse(rowObjects(1).asInstanceOf[String].trim, DateTimeFormat.forPattern("HH:mm:ss"))
                  val start = startDate.toLocalDateTime(startTime)
                  val acftID = rowObjects(2).asInstanceOf[String]
                  val operation = rowObjects(3).asInstanceOf[String]
                  val runway = rowObjects(4).asInstanceOf[String]
                  val flightRoute = rowObjects(5).asInstanceOf[String]

                  val record = FlightInfo(start.toDate, acftID: String, operation: String, runway: String, flightRoute: String)
                  recordList = recordList :+ (record)
                } catch {
                  case ex: Exception =>
                    val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "",
                      dataType = s"$relativePath",
                      fieldName = "", errorInfo = "資料格式錯誤", value = ex.getMessage)
                    dfeList = dfeList:+(dfe)
                    Logger.error(s"${path.toFile.getPath} 忽略第${count}筆錯誤資料", ex)
                }
              }
            } while (rowObjects != null)
            if (recordList.nonEmpty) {
              val collection = reportRecordOp.getFlightCollection
              val f = collection.insertMany(recordList).toFuture()
              f onFailure (errorHandler)
            } else {
              val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "",
                dataType = s"$relativePath",
                fieldName = "", errorInfo = "檔案無資料", value = "")
              dfeList = dfeList:+(dfe)
            }
          } catch {
            case ex: Exception =>
              val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "",
                dataType = s"$relativePath",
                fieldName = "", errorInfo = "檔案無法讀取", value = "")
              dfeList = dfeList:+(dfe)
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

  def getTerminalMap(): Future[Map[Int, String]] = {
    val infoID = reportInfo._id.airpotInfoID
    val f = airportInfoOp.get(infoID.airportID, infoID.year, infoID.quarter)
    for (ret <- f) yield {
      val pairs = ret(0).terminals.map(t => t.no -> t.name)
      pairs.toMap
    }
  }

  def importDbfPhase(mainFolder: String, importTasks: Int, terminalMap: Map[Int, String]): Receive = {

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
      self ! ImportDailyFlight

    case ImportSecondWind =>
      context become importDbfPhase(mainFolder, importTasks + 1,  terminalMap)
      importSecData(mainFolder, "每秒風速監測資料")

    case ImportSecondNoise =>
      context become importDbfPhase(mainFolder, importTasks + 1,  terminalMap)
      importSecData(mainFolder, "每秒噪音監測資料")

    case ImportHourlyNoise =>
      context become importDbfPhase(mainFolder, importTasks + 1,  terminalMap)
      importNoiseData(mainFolder, "每小時噪音監測資料", ReportRecord.Hour)

    case ImportDailyNoise =>
      context become importDbfPhase(mainFolder, importTasks + 1,  terminalMap)
      importNoiseData(mainFolder, "每日噪音監測資料", ReportRecord.Day)

    case ImportMonthlyNoise =>
      context become importDbfPhase(mainFolder, importTasks + 1,  terminalMap)
      importNoiseData(mainFolder, "每月噪音監測資料", ReportRecord.Month)

    case ImportQuarterNoise =>
      context become importDbfPhase(mainFolder, importTasks + 1,  terminalMap)
      importNoiseData(mainFolder, "每季噪音監測資料", ReportRecord.Quarter)

    case ImportYearlyNoise =>
      context become importDbfPhase(mainFolder, importTasks + 1,  terminalMap)
      importNoiseData(mainFolder, "一年噪音監測資料", ReportRecord.Year)

    case ImportHourlyWeather =>
      context become importDbfPhase(mainFolder, importTasks + 1,  terminalMap)
      importWindHourData(mainFolder, "每小時氣象噪音監測資料")

    case ImportNoiseEvent =>
      context become importDbfPhase(mainFolder, importTasks + 1,  terminalMap)
      importEventData(mainFolder, "噪音事件監測資料", reportRecordOp.getEventCollection)

    case ImportTestNoiseEvent =>
      context become importDbfPhase(mainFolder, importTasks + 1,  terminalMap)
      importEventData(mainFolder, "試車噪音監測資料", reportRecordOp.getTestEventCollection)

    case ImportDailyFlight =>
      context become importDbfPhase(mainFolder, importTasks + 1,  terminalMap)
      importFlightData(mainFolder, "每日飛航監測資料")

    case TaskComplete =>
      if (importTasks - 1 != 0)
        context become importDbfPhase(mainFolder, importTasks - 1,  terminalMap)
      else {
        context become auditReportPhase(mainFolder, 0)
        reportInfoOp.clearAllSubTasks(reportInfo._id)
        FileUtils.deleteDirectory(new File(mainFolder))
        self ! AuditReport
      }
    case TaskAbort(reason) =>
      reportInfo.state = "失敗"
      reportInfo.appendUnableAuditReason(reason)
      reportInfoOp.upsertReportInfo(reportInfo)
      finish(context.self.path.name)
      self ! PoisonPill
  }

  def auditReportPhase(mainFolder: String, auditTasks: Int): Receive = {
    case AuditReport =>
      reportInfoOp.updateState(reportInfo._id, "產出稽核報告中")
      context become auditReportPhase(mainFolder, 1)
      self ! TaskComplete
    case TaskComplete =>
      if (auditTasks - 1 != 0)
        context become auditReportPhase(mainFolder, auditTasks - 1)
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
