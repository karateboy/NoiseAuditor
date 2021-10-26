package models

import akka.actor._
import com.github.nscala_time.time.Imports._
import com.linuxense.javadbf.{DBFRow, DBFUtils}
import models.ModelHelper._
import models.ReportRecord.Noise
import org.apache.commons.io.FileUtils
import org.mongodb.scala.MongoCollection
import play.api._

import java.io.{BufferedInputStream, File}
import java.nio.file.{Files, NoSuchFileException, Path, Paths}
import scala.collection.JavaConverters._
import scala.concurrent._
import scala.util.{Failure, Success}

object ReportImporter {
  var n = 0
  private var actorRefMap = Map.empty[String, ActorRef]

  def start(dataFile: File, airportInfoOp: AirportInfoOp, reportInfo: ReportInfo,
            reportInfoOp: ReportInfoOp, reportRecordOp: ReportRecordOp, importErrorLogsOp: ImportErrorLogOp,
            reportTolerance: ReportTolerance, auditLogOp: AuditLogOp)(implicit actorSystem: ActorSystem, ec: ExecutionContextExecutor) = {
    val name = getName
    val actorRef = actorSystem.actorOf(ReportImporter.props(dataFile = dataFile, airportInfoOp = airportInfoOp,
      reportInfo = reportInfo, reportInfoOp = reportInfoOp, reportRecordOp, importErrorLogsOp, reportTolerance = reportTolerance,
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
            reportInfoOp: ReportInfoOp, reportRecordOp: ReportRecordOp, importErrorLogsOp: ImportErrorLogOp,
            reportTolerance: ReportTolerance, auditLogOp: AuditLogOp) =
    Props(classOf[ReportImporter], dataFile, airportInfoOp, reportInfo,
      reportInfoOp, importErrorLogsOp, reportRecordOp, reportTolerance, auditLogOp)

  def reaudit(airportInfoOp: AirportInfoOp, reportInfo: ReportInfo, reportInfoOp: ReportInfoOp,
              importErrorLogsOp: ImportErrorLogOp, reportRecordOp: ReportRecordOp,
              reportTolerance: ReportTolerance, auditLogOp: AuditLogOp)
             (implicit actorSystem: ActorSystem, ec: ExecutionContextExecutor) = {
    val name = getName
    val actorRef = actorSystem.actorOf(ReportImporter.props(dataFile = new File(""), airportInfoOp = airportInfoOp,
      reportInfo = reportInfo, reportInfoOp = reportInfoOp, reportRecordOp = reportRecordOp, importErrorLogsOp = importErrorLogsOp,
      reportTolerance = reportTolerance, auditLogOp = auditLogOp), name)
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

  //case object ImportSecondWind

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
                     reportInfo: ReportInfo, reportInfoOp: ReportInfoOp, importErrorLogsOp: ImportErrorLogOp,
                     reportRecordOp: ReportRecordOp, reportTolerance: ReportTolerance, auditLogOp: AuditLogOp) extends Actor {

  import ReportImporter._

  implicit val ec: ExecutionContextExecutorService = {
    import java.util.concurrent.Executors
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors() / 2))
  }

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
    val paths: List[Path] = try {
      Files.list(Paths.get(mainFolder, relativePath)).iterator().asScala.toList
    } catch {
      case ex: NoSuchFileException =>
        Logger.error("s\"$mainFolder\\\\$relativePath no such files")
        List.empty[Path]
    }
    val ret = paths.filter(!Files.isDirectory(_)).filter(p => p.toFile.getAbsolutePath.endsWith("dbf"))
    Logger.info(s"$mainFolder\\$relativePath #=${ret.size}")
    ret
  }

  def importNoiseSecData(mainFolder: String, relativePath: String) = {
    def handleFile(path: Path, taskName: String): Boolean = {
      var dfeList = List.empty[DataFormatError]
      val recordType = Noise
      import com.linuxense.javadbf.DBFReader
      var reader: DBFReader = null
      var recordList = Seq.empty[MinRecord]
      var dbfRow: DBFRow = null
      try {
        Logger.debug(s"Handle ${path.toAbsolutePath.toString}")
        reader = new DBFReader(Files.newInputStream(path))
        //reader = new DBFReader(Files.newInputStream(path))
        var row = 0
        do {
          dbfRow = reader.nextRow()
          if (dbfRow != null) {
            row = row + 1
            var terminal = ""
            var time = ""
            var fieldName = ""
            var reason = ""
            try {
              val nmtNumber = try {
                dbfRow.getString("NMT_NUMBER").trim.toInt
              } catch {
                case ex: Throwable =>
                  fieldName = "NMT_NUMBER"
                  reason = s"第${row}行格式錯誤"
                  throw ex
              }
              if (terminalMap.contains(nmtNumber))
                terminal = terminalMap(nmtNumber)
              else {
                fieldName = "NMT_NUMBER"
                reason = s"未知的NMT_NUMBER $nmtNumber"
                throw new Exception(s"$nmtNumber")
              }
              val startDate: LocalDate = try {
                try {
                  new LocalDate(dbfRow.getDate("START_DATE"))
                } catch {
                  case _: Throwable =>
                    val dateStr = dbfRow.getString("START_DATE").trim
                    if (dateStr.contains("-"))
                      LocalDate.parse(dbfRow.getString("START_DATE").trim, DateTimeFormat.forPattern("yyyy-MM-dd"))
                    else
                      LocalDate.parse(dbfRow.getString("START_DATE").trim, DateTimeFormat.forPattern("yyyy/MM/dd"))
                }
              } catch {
                case ex: Throwable =>
                  fieldName = "START_DATE"
                  reason = s"第${row}行格式錯誤"
                  throw ex
              }

              val startTime = try {
                val timeStr = dbfRow.getString("START_TIME").trim
                if (timeStr.length > 5)
                  LocalTime.parse(dbfRow.getString("START_TIME").trim, DateTimeFormat.forPattern("HH:mm:ss"))
                else
                  LocalTime.parse(dbfRow.getString("START_TIME").trim, DateTimeFormat.forPattern("HH:mm"))
              } catch {
                case ex: Throwable =>
                  fieldName = "START_TIME"
                  reason = s"第${row}行格式錯誤"
                  throw ex
              }
              val start = startDate.toLocalDateTime(startTime)
              time = start.toString("yyyy/MM/dd HH:mm:ss")
              var secRecords = Seq.empty[SecRecord]
              for (i <- 0 to 59) {
                try {
                  secRecords = secRecords :+ SecRecord(start.plusSeconds(i).toDate(), dbfRow.getDouble(s"SL$i"))
                } catch {
                  case _: Throwable =>
                }
              }
              val record = MinRecord(RecordID(start.toDate, nmtNumber, recordType.toString), secRecords)
              recordList = recordList :+ (record)
            } catch {
              case ex: Exception =>
                val dfe = DataFormatError(fileName = path.toFile.getName, terminal = terminal, time = time,
                  dataType = s"$relativePath",
                  fieldName = fieldName, errorInfo = reason, value = ex.getMessage)
                dfeList = dfeList :+ (dfe)
            }
          }
        } while (dbfRow != null)
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
          val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "",
            dataType = s"$relativePath",
            fieldName = "", errorInfo = "無法讀取資料", value = "")
          dfeList = dfeList :+ (dfe)
          reportInfo.appendUnableAuditReason(s"檔案格式錯誤 ${relativePath} ${path.toFile.getName}")
          Logger.error(s"無法匯入 ${path.toFile.getPath}", ex)
      } finally {
        DBFUtils.close(reader)
      }

      if (dfeList.nonEmpty) {
        val id = ImportLogID(reportInfo._id, path.toFile.getAbsolutePath)
        importErrorLogsOp.upsert(ImportErrorLog(id, dfeList.take(100)))
      }
      reportInfoOp.incSubTaskCurrentCount(reportInfo._id, taskName)
      Logger.debug(s"${path.toAbsolutePath.toString} complete.")
      true
    }

    val fileCount = getDbfList(mainFolder, relativePath)
    val subTask = SubTask(s"匯入${relativePath}", 0, fileCount.size)
    reportInfoOp.addSubTask(reportInfo._id, subTask)

    val futureList: List[Future[Boolean]] = List(
      Future {
        blocking {
          for (path <- fileCount) yield {
            handleFile(path, subTask.name)
          }
          true
        }
      }
    )


    val allF = Future.sequence(futureList)
    allF onFailure errorHandler
    allF onComplete ({
      case Success(value) =>
        self ! TaskComplete
      case Failure(exception) =>
        Logger.error(s"Task failed $relativePath", exception)
        self ! TaskAbort(s"匯入$relativePath 失敗")
    })
  }

  import ReportRecord._

  def importNoiseData(mainFolder: String, relativePath: String, recordPeriod: RecordPeriod) = {
    def handleFile(path: Path, taskName: String) = {
      var dfeList = List.empty[DataFormatError]
      import com.linuxense.javadbf.DBFReader
      var reader: DBFReader = null
      var rowObjects: Array[Object] = null
      try {
        reader = new DBFReader(new BufferedInputStream(Files.newInputStream(path)))
        //reader = new DBFReader(Files.newInputStream(path))
        reportInfoOp.incSubTaskCurrentCount(reportInfo._id, taskName)
        var row = 0
        do {
          rowObjects = reader.nextRecord()
          if (rowObjects != null) {
            row = row + 1
            var terminal = ""
            var time = ""
            var fieldName = ""
            var reason = ""
            try {
              val nmtNumber = try {
                rowObjects(0).asInstanceOf[String].trim.toInt
              } catch {
                case ex: Throwable =>
                  fieldName = "NMT_NUMBER"
                  reason = s"第${row}行格式錯誤"
                  throw ex
              }
              val nmtName = try {
                rowObjects(1).asInstanceOf[String].trim
              } catch {
                case ex: Throwable =>
                  fieldName = "NMT_NAME"
                  reason = s"格式錯誤"
                  throw ex
              }
              terminal = nmtName
              if (terminalMap.contains(nmtNumber))
                terminal = terminalMap(nmtNumber)
              else {
                fieldName = "NMT_NUMBER"
                reason = s"未知的NMT_NUMBER $nmtNumber"
                throw new Exception(s"$nmtNumber")
              }

              val startDate = try {
                LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy-MM-dd"))
              } catch {
                case _: IllegalArgumentException =>
                  try {
                    LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy/MM/dd"))
                  } catch {
                    case ex: Exception =>
                      fieldName = "START_DATE"
                      reason = s"格式錯誤"
                      throw ex
                  }
              }
              val startTime = try {
                LocalTime.parse(rowObjects(3).asInstanceOf[String].trim, DateTimeFormat.forPattern("HH:mm:ss"))
              } catch {
                case ex: Throwable =>
                  fieldName = "START_TIME"
                  reason = s"格式錯誤"
                  throw ex
              }

              val start = startDate.toLocalDateTime(startTime)
              val activity = try {
                rowObjects(4).asInstanceOf[java.math.BigDecimal].toBigInteger.intValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "ACTIVITY"
                  reason = s"格式錯誤"
                  throw ex
              }
              val totalEvent = try {
                rowObjects(5).asInstanceOf[java.math.BigDecimal].doubleValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "TOTAL_EVENT_SEL"
                  reason = s"格式錯誤"
                  throw ex
              }
              val totalLeq = try {
                rowObjects(6).asInstanceOf[java.math.BigDecimal].doubleValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "TOTAL_Leq"
                  reason = s"格式錯誤"
                  throw ex
              }
              val eventLeq = try {
                rowObjects(7).asInstanceOf[java.math.BigDecimal].doubleValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "EVENT_Leq"
                  reason = s"格式錯誤"
                  throw ex
              }
              val backLeq = try {
                rowObjects(8).asInstanceOf[java.math.BigDecimal].doubleValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "BACK_Leq"
                  reason = s"格式錯誤"
                  throw ex
              }
              val totalLdn = try {
                rowObjects(9).asInstanceOf[java.math.BigDecimal].doubleValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "TOTAL_Ldn"
                  reason = s"格式錯誤"
                  throw ex
              }
              val eventLdn = try {
                rowObjects(10).asInstanceOf[java.math.BigDecimal].doubleValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "EVENT_Ldn"
                  reason = s"格式錯誤"
                  throw ex
              }
              val backLdn = try {
                rowObjects(11).asInstanceOf[java.math.BigDecimal].doubleValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "BACK_Ldn"
                  reason = s"格式錯誤"
                  throw ex
              }
              val l5 = try {
                rowObjects(12).asInstanceOf[java.math.BigDecimal].doubleValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "L5"
                  reason = s"格式錯誤"
                  throw ex
              }
              val l10 = try {
                rowObjects(13).asInstanceOf[java.math.BigDecimal].doubleValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "L10"
                  reason = s"格式錯誤"
                  throw ex
              }
              val l50 = try {
                rowObjects(14).asInstanceOf[java.math.BigDecimal].doubleValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "L50"
                  reason = s"格式錯誤"
                  throw ex
              }
              val l90 = try {
                rowObjects(15).asInstanceOf[java.math.BigDecimal].doubleValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "L90"
                  reason = s"格式錯誤"
                  throw ex
              }
              val l95 = try {
                rowObjects(16).asInstanceOf[java.math.BigDecimal].doubleValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "L95"
                  reason = s"格式錯誤"
                  throw ex
              }
              val l99 = try {
                rowObjects(17).asInstanceOf[java.math.BigDecimal].doubleValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "L99"
                  reason = s"格式錯誤"
                  throw ex
              }
              val numEvent = try {
                rowObjects(18).asInstanceOf[java.math.BigDecimal].toBigInteger.intValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "NUM_OF_EVENT"
                  reason = s"格式錯誤"
                  throw ex
              }
              val duration = try {
                rowObjects(19).asInstanceOf[java.math.BigDecimal].toBigInteger.intValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "DURATION"
                  reason = s"格式錯誤"
                  throw ex
              }

              val record = NoiseRecord(RecordID(start.toDate, nmtNumber, Noise.toString), activity = activity,
                totalEvent = totalEvent,
                totalLeq = totalLeq, eventLeq = eventLeq, backLeq = backLeq, totalLdn = totalLdn,
                eventLdn = eventLdn, backLdn = backLdn,
                l5 = l5, l10 = l10, l50 = l50, l90 = l90, l95 = l95, l99 = l99, numEvent = numEvent, duration = duration)

              val collection = reportRecordOp.getNoiseCollection(recordPeriod)
              val f = collection.insertOne(record).toFuture()
              f onFailure ({
                case ex: Throwable =>
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
            fieldName = "", errorInfo = "無法讀取資料", value = "")
          dfeList = dfeList :+ (dfe)
          reportInfo.appendUnableAuditReason(s"檔案格式錯誤 ${relativePath} ${path.toFile.getName}")
          Logger.error(s"無法匯入 ${path.toFile.getPath}", ex)
      } finally {
        DBFUtils.close(reader)
      }
      if (dfeList.nonEmpty) {
        val id = ImportLogID(reportInfo._id, s"$relativePath")
        importErrorLogsOp.appendDataFormatErrors(id, dfeList)
      }
      true
    }

    val fileCount = getDbfList(mainFolder, relativePath)
    val subTask = SubTask(s"匯入${relativePath}", 0, fileCount.size)
    reportInfoOp.addSubTask(reportInfo._id, subTask)
    val futureList =
      for (path <- fileCount) yield {
        Future {
          blocking(handleFile(path, subTask.name))
        }
      }

    val allF = Future.sequence(futureList)
    allF onFailure errorHandler
    allF onComplete ({
      case Success(_) =>
        self ! TaskComplete
      case Failure(exception) =>
        Logger.error(s"Task failed $relativePath", exception)
        self ! TaskAbort(s"匯入$relativePath 失敗")
    })
  }

  def importWindHourData(mainFolder: String, relativePath: String) = {
    def handleFile(path: Path, taskName: String) = {
      var dfeList = List.empty[DataFormatError]
      import com.linuxense.javadbf.DBFReader
      var reader: DBFReader = null
      var rowObjects: Array[Object] = null
      var row = 0
      try {
        reader = new DBFReader(new BufferedInputStream(Files.newInputStream(path)))
        // reader = new DBFReader(Files.newInputStream(path))
        do {
          rowObjects = reader.nextRecord()
          if (rowObjects != null) {
            row = row + 1
            var terminal = ""
            var time = ""
            var fieldName = ""
            var reason = ""
            try {
              val nmtNumber = try {
                rowObjects(0).asInstanceOf[String].trim.toInt
              } catch {
                case ex: Throwable =>
                  fieldName = "NMT_NUMBER"
                  reason = s"第${row}行格式錯誤"
                  throw ex
              }
              val nmtName = try {
                rowObjects(1).asInstanceOf[String].trim
              } catch {
                case ex: Throwable =>
                  fieldName = "NMT_NAME"
                  reason = s"格式錯誤"
                  throw ex
              }
              terminal = nmtName
              if (terminalMap.contains(nmtNumber))
                terminal = terminalMap(nmtNumber)
              else {
                fieldName = "NMT_NUMBER"
                reason = s"未知的NMT_NUMBER $nmtNumber"
                throw new Exception(s"$nmtNumber")
              }

              val startDate = try {
                LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy-MM-dd"))
              } catch {
                case _: IllegalArgumentException =>
                  try {
                    LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy/MM/dd"))
                  } catch {
                    case ex: Exception =>
                      fieldName = "START_DATE"
                      reason = s"格式錯誤"
                      throw ex
                  }
              }
              val startTime = try {
                LocalTime.parse(rowObjects(3).asInstanceOf[String].trim, DateTimeFormat.forPattern("HH:mm:ss"))
              } catch {
                case ex: Throwable =>
                  fieldName = "START_TIME"
                  reason = s"格式錯誤"
                  throw ex
              }

              val start = startDate.toLocalDateTime(startTime)
              val windSpeedAvg = try {
                rowObjects(5).asInstanceOf[java.math.BigDecimal].doubleValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "MEEN_WINDSPEED"
                  reason = s"格式錯誤"
                  throw ex
              }
              val windSpeedMax = try {
                rowObjects(6).asInstanceOf[java.math.BigDecimal].doubleValue()
              } catch {
                case ex: Throwable =>
                  fieldName = "MAX_WINDSPEED"
                  reason = s"格式錯誤"
                  throw ex
              }
              val record = WindHourRecord(RecordID(start.toDate, nmtNumber, WindSpeed.toString),
                windSpeedAvg, windSpeedMax)
              val collection = reportRecordOp.getWindHourCollection
              val f = collection.insertOne(record).toFuture()
              f onFailure {
                case ex: Throwable =>
                  val recordTerminal = terminalMap(record._id.terminalID)
                  val time = new DateTime(record._id.time).toString("YYYY/MM/dd HH:mm:ss")
                  val dfe = DataFormatError(fileName = path.toFile.getName, terminal = recordTerminal,
                    time = time,
                    dataType = s"$relativePath",
                    fieldName = fieldName, errorInfo = "資料重複", value = ex.getMessage)
                  dfeList = dfeList :+ (dfe)
              }
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
            fieldName = "", errorInfo = "無法讀取資料", value = "")
          dfeList = dfeList :+ (dfe)
          reportInfo.appendUnableAuditReason(s"檔案格式錯誤 ${relativePath} ${path.toFile.getName}")
          Logger.error(s"無法匯入 ${path.toFile.getPath}", ex)
      } finally {
        DBFUtils.close(reader)
      }
      if (dfeList.nonEmpty) {
        val id = ImportLogID(reportInfo._id, s"$relativePath")
        importErrorLogsOp.appendDataFormatErrors(id, dfeList)
      }
      reportInfoOp.incSubTaskCurrentCount(reportInfo._id, taskName)
      true
    }

    val fileCount = getDbfList(mainFolder, relativePath)
    val subTask = SubTask(s"匯入${relativePath}", 0, fileCount.size)
    reportInfoOp.addSubTask(reportInfo._id, subTask)
    val futureList =
      for (path <- fileCount) yield
        Future {
          blocking(handleFile(path, subTask.name))
        }

    val allF = Future.sequence(futureList)
    allF onFailure errorHandler
    allF onComplete ({
      case Success(_) =>
        self ! TaskComplete
      case Failure(exception) =>
        Logger.error(s"Task failed $relativePath", exception)
        self ! TaskAbort(s"匯入$relativePath 失敗")
    })
  }

  def importEventData(mainFolder: String, relativePath: String, collection: MongoCollection[EventRecord]) = {
    def handleEventFile(path: Path, taskName: String) = {
      import com.linuxense.javadbf.DBFReader
      var reader: DBFReader = null
      var dbfRow: DBFRow = null
      var terminal = ""
      var time = ""
      var fieldName = ""
      var reason = ""
      var dfeList = List.empty[DataFormatError]
      try {
        reader = new DBFReader(new BufferedInputStream(Files.newInputStream(path)))
        //reader = new DBFReader(Files.newInputStream(path))
        reportInfoOp.incSubTaskCurrentCount(reportInfo._id, taskName)
        var row = 0
        do {
          dbfRow = reader.nextRow()
          row = row + 1
          terminal = ""
          time = ""
          fieldName = ""
          reason = ""
          if (dbfRow != null) {
            try {
              val nmtNumber = try {
                dbfRow.getString("NMT_NUMBER").trim.toInt
              } catch {
                case ex: Throwable =>
                  fieldName = "NMT_NUMBER"
                  reason = s"第${row}行格式錯誤"
                  throw ex
              }
              if (terminalMap.contains(nmtNumber))
                terminal = terminalMap(nmtNumber)
              else {
                fieldName = "NMT_NUMBER"
                reason = s"未知的NMT_NUMBER $nmtNumber"
                throw new Exception(s"$nmtNumber")
              }
              val startDate: LocalDate = try {
                LocalDate.parse(dbfRow.getString("START_DATE").trim, DateTimeFormat.forPattern("yyyy-MM-dd"))
              } catch {
                case _: IllegalArgumentException =>
                  try {
                    LocalDate.parse(dbfRow.getString("START_DATE").trim, DateTimeFormat.forPattern("yyyy/MM/dd"))
                  } catch {
                    case ex: Throwable =>
                      try {
                        new LocalDate(dbfRow.getDate("START_DATE"))
                      } catch {
                        case ex: Throwable =>
                          fieldName = "START_DATE"
                          reason = s"第${row}行格式錯誤"
                          throw ex
                      }
                  }
              }

              val startTime = try {
                LocalTime.parse(dbfRow.getString("START_TIME").trim, DateTimeFormat.forPattern("HH:mm:ss"))
              } catch {
                case ex: Throwable =>
                  fieldName = "START_TIME"
                  reason = s"第${row}行格式錯誤"
                  throw ex
              }
              val start = startDate.toLocalDateTime(startTime)
              time = start.toString("yyyy/MM/dd HH:mm:ss")
              val duration = try {
                dbfRow.getInt("DURATION_T")
              } catch {
                case ex: Throwable =>
                  fieldName = "DURATION_T"
                  reason = s"第${row}行格式錯誤"
                  throw ex
              }
              val setl = try {
                dbfRow.getDouble("SETL")
              } catch {
                case ex: Throwable =>
                  fieldName = "SETL"
                  reason = s"第${row}行格式錯誤"
                  throw ex
              }
              val minDurationTime = try {
                dbfRow.getInt("MIN_DUR_TI")
              } catch {
                case ex: Throwable =>
                  fieldName = "MIN_DUR_TIME"
                  reason = s"第${row}行格式錯誤"
                  throw ex
              }
              val eventLeq = try {
                dbfRow.getDouble("EVENT_LEQ")
              } catch {
                case ex: Throwable =>
                  fieldName = "EVENT_Leq"
                  reason = s"第${row}行格式錯誤"
                  throw ex
              }
              val eventSel = try {
                dbfRow.getDouble("EVENT_SEL")
              } catch {
                case ex: Throwable =>
                  fieldName = "EVENT_SEL"
                  reason = s"第${row}行格式錯誤"
                  throw ex
              }
              val eventMaxLen = try {
                dbfRow.getDouble("EVENT_MAXL")
              } catch {
                case ex: Throwable =>
                  fieldName = "EVENT_MAX_LEVEL"
                  reason = s"第${row}行格式錯誤"
                  throw ex
              }
              val eventMaxT = try {
                LocalTime.parse(dbfRow.getString("EVENT_MAXT"), DateTimeFormat.forPattern("HH:mm:ss"))
              } catch {
                case ex: Throwable =>
                  fieldName = "EVENT_MAX_TIME"
                  reason = s"第${row}行格式錯誤"
                  throw ex
              }
              var secRecords = Seq.empty[SecRecord]
              for (i <- 0 to 122) {
                try {
                  secRecords = secRecords :+ SecRecord(start.plusSeconds(i).toDate(), dbfRow.getDouble(s"SL$i"))
                } catch {
                  case _: Exception =>
                }
              }
              val record = EventRecord(RecordID(start.toDate, nmtNumber, Event.toString), duration = duration,
                setl = setl, minDur = minDurationTime, eventLeq = eventLeq, eventSel = eventSel, eventMaxLen = eventMaxLen,
                eventMaxTime = startDate.toLocalDateTime(eventMaxT).toDate(), secRecords)
              val f = collection.insertOne(record).toFuture()
              f onFailure ({
                case ex: Throwable =>
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
        } while (dbfRow != null)
      } catch {
        case ex: Exception =>
          val dfe = DataFormatError(fileName = path.toFile.getName, terminal = "", time = "",
            dataType = s"$relativePath",
            fieldName = "", errorInfo = "檔案格式錯誤", value = "")
          dfeList = dfeList :+ (dfe)
          reportInfo.appendUnableAuditReason(s"檔案格式錯誤 ${relativePath} ${path.toFile.getName}")
          Logger.error(s"無法匯入 ${path.toFile.getPath}", ex)
      } finally {
        DBFUtils.close(reader)
      }
      if (dfeList.nonEmpty) {
        val id = ImportLogID(reportInfo._id, path.toFile.getAbsolutePath)
        importErrorLogsOp.upsert(ImportErrorLog(id, dfeList.take(100)))
      }
      true
    }

    val fileCount = getDbfList(mainFolder, relativePath)
    val subTask = SubTask(s"匯入${relativePath}", 0, fileCount.size)
    reportInfoOp.addSubTask(reportInfo._id, subTask)
    val futureList: List[Future[Boolean]] =
      for (path <- fileCount) yield
        Future {
          blocking {
            handleEventFile(path, subTask.name)
          }
        }

    val allF = Future.sequence(futureList)
    allF onFailure errorHandler
    allF onComplete ({
      case Success(_) =>
        self ! TaskComplete
      case Failure(exception) =>
        Logger.error(s"Task failed $relativePath", exception)
        self ! TaskAbort(s"匯入$relativePath 失敗")
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
            // reader = new DBFReader(Files.newInputStream(path))
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
                      try {
                        LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy/MM/dd"))

                      } catch {
                        case ex: Throwable =>
                          fieldName = "START_DATE"
                          reason = s"第${row}行格式錯誤"
                          throw ex
                      }
                  }
                  val startTime = try {
                    LocalTime.parse(rowObjects(3).asInstanceOf[String].trim, DateTimeFormat.forPattern("HH:mm:ss"))
                  } catch {
                    case ex: Throwable =>
                      fieldName = "START_TIME"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }
                  val start = startDate.toLocalDateTime(startTime)
                  time = start.toString("yyyy/MM/dd HH:mm:ss")
                  val acftID = try {
                    rowObjects(2).asInstanceOf[String]
                  } catch {
                    case ex: Throwable =>
                      fieldName = "ACFT_ID"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }
                  val operation = try {
                    rowObjects(3).asInstanceOf[String]
                  } catch {
                    case ex: Throwable =>
                      fieldName = "OPERATION"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }
                  val runway = try {
                    rowObjects(4).asInstanceOf[String]
                  } catch {
                    case ex: Throwable =>
                      fieldName = "RUNWAY"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }
                  val flightRoute = try {
                    rowObjects(5).asInstanceOf[String]
                  } catch {
                    case ex: Throwable =>
                      fieldName = "FLIGHT_ROUTE"
                      reason = s"第${row}行格式錯誤"
                      throw ex
                  }

                  val record = FlightInfo(start.toDate, acftID: String, operation: String, runway: String, flightRoute: String)
                  val collection = reportRecordOp.getFlightCollection
                  val f = collection.insertOne(record).toFuture()
                  f onFailure ({
                    case ex: Throwable =>
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
        val id = ImportLogID(reportInfo._id, s"$relativePath")
        importErrorLogsOp.appendDataFormatErrors(id, dfeList)
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


      self ! ImportHourlyNoise
      self ! ImportHourlyWeather
      self ! ImportDailyNoise
      self ! ImportMonthlyNoise
      self ! ImportQuarterNoise
      self ! ImportYearlyNoise
      //self ! ImportTestNoiseEvent
      self ! ImportNoiseEvent
      self ! ImportSecondNoise
    // self ! ImportDailyFlight

    case ImportSecondNoise =>
      context become importDbfPhase(mainFolder, importTasks + 1)
      importNoiseSecData(mainFolder, "每秒噪音監測資料")

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
        try {
          FileUtils.deleteDirectory(new File(mainFolder))
        } catch {
          case _: Throwable =>
        }
        reportInfo.state = "匯入完畢"
        reportInfoOp.upsertReportInfo(reportInfo)
        finish(context.self.path.name)
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
      val d: Duration = new Duration(start, end)
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

  override def postStop(): Unit = {
    super.postStop()
  }
}
