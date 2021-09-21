package models

import akka.actor._
import com.linuxense.javadbf.DBFUtils
import models.ModelHelper._
import models.ReportRecord.{Noise, RecordPeriod, WindDirection, WindSpeed}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{LocalDate, LocalTime}
import play.api._

import java.io.{BufferedInputStream, File}
import java.nio.file.{Files, Path, Paths}
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, blocking}

object ReportImporter {
  var n = 0
  private var actorRefMap = Map.empty[String, ActorRef]

  def start(dataFile: File, reportInfo: ReportInfo, reportInfoOp: ReportInfoOp, reportRecordOp: ReportRecordOp)(implicit actorSystem: ActorSystem) = {
    val name = getName
    val actorRef = actorSystem.actorOf(ReportImporter.props(dataFile = dataFile,
      reportInfo = reportInfo, reportInfoOp, reportRecordOp), name)
    actorRefMap = actorRefMap + (name -> actorRef)
    name
  }

  def getName = {
    n = n + 1
    s"dataImporter${n}"
  }

  def props(dataFile: File, reportInfo: ReportInfo, reportInfoOp: ReportInfoOp, reportRecordOp: ReportRecordOp) =
    Props(classOf[ReportImporter], dataFile, reportInfo, reportInfoOp, reportRecordOp)

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

class ReportImporter(dataFile: File, reportInfo: ReportInfo, reportInfoOp: ReportInfoOp, reportRecordOp: ReportRecordOp) extends Actor {

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
              reportInfo.appendImportLog(message)
              reportInfoOp.upsertReportInfo(reportInfo)
              self ! TaskComplete
            } else {
              val message = "解壓縮成功"
              reportInfo.state = message
              reportInfo.appendImportLog(message)
              reportInfoOp.upsertReportInfo(reportInfo)
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

    case TaskAbort(reason) =>
      reportInfo.state = "失敗"
      reportInfo.appendImportLog(reason)
      reportInfoOp.upsertReportInfo(reportInfo)
      finish(context.self.path.name)
      self ! PoisonPill
  }

  def getDbfList(mainFolder: String, relativePath: String): List[Path] = {
    val paths: List[Path] = Files.list(Paths.get(mainFolder, relativePath)).iterator().asScala.toList
    val ret = paths.filter(!Files.isDirectory(_))
    Logger.info(s"$mainFolder\\$relativePath #=${ret.size}")
    ret
  }

  def importSecData(mainFolder:String, relativePath:String)={
    Future {
      blocking {
        val fileCount = getDbfList(mainFolder, relativePath)
        var count = 0
        val subTask = SubTask(s"匯入${relativePath}", count, fileCount.size)
        reportInfoOp.addSubTask(reportInfo._id, subTask)
        for (path <- fileCount) {
          val recordType =
            if (path.toFile.getName.contains("wd"))
              WindDirection
            else if(path.toFile.getName.contains("ws"))
              WindSpeed
            else
              Noise

          Logger.info(s"處理 ${path.toFile.getName}")
          import com.linuxense.javadbf.DBFReader
          var reader: DBFReader = null
          var recordList = Seq.empty[MinRecord]
          var rowObjects: Array[Object] = null
          try {
            reader = new DBFReader(new BufferedInputStream(Files.newInputStream(path)))
            count = count + 1
            subTask.current = count
            reportInfoOp.updateSubTask(reportInfo._id, subTask)

            do {
              rowObjects = reader.nextRecord()
              if (rowObjects != null) {
                try {
                  val mntNumber = rowObjects(0).asInstanceOf[String].trim.toInt
                  val mntName = rowObjects(1).asInstanceOf[String].trim
                  val startDate = LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy-MM-dd"))
                  val startTime = LocalTime.parse(rowObjects(3).asInstanceOf[String].trim, DateTimeFormat.forPattern("HH:mm"))
                  val start = startDate.toLocalDateTime(startTime)
                  var secRecords = Seq.empty[SecRecord]
                  for (i <- 0 to 59) {
                    try {
                      secRecords = secRecords :+ SecRecord(start.plusSeconds(i).toDate(), rowObjects(i + 5).asInstanceOf[java.math.BigDecimal].doubleValue())
                    } catch {
                      case ex: Exception =>
                        Logger.error(s"${path.toFile.getName} ${mntName} ${start} 遺失第${i}秒資料")
                    }
                  }
                  val record = MinRecord(RecordID(start.toDate, mntNumber, recordType.toString), secRecords)
                  recordList = recordList :+ (record)
                } catch {
                  case ex: Exception =>
                    Logger.error(s"${path.toFile.getName} 忽略第${count}筆錯誤資料")
                }
              }
            } while (rowObjects != null)
            val collection = reportRecordOp.getMinCollection(recordType)
            val f = collection.insertMany(recordList).toFuture()
            f onFailure (errorHandler)
            Logger.info(s"成功匯入${path.toFile.getName}")
          } catch {
            case ex: Exception =>
              Logger.error(s"無法匯入 ${path.toFile.getName}", ex)
          } finally {
            DBFUtils.close(reader)
          }
        }
        self ! TaskComplete
      }
    } onFailure ({
      case ex: Exception =>
        Logger.error(s"failed to import ${relativePath}", ex)
        self ! TaskAbort(s"無法匯入${ relativePath}")
    })
  }

  def importNoiseData(mainFolder:String, relativePath:String, recordPeriod: RecordPeriod)={
    Future {
      blocking {
        val fileCount = getDbfList(mainFolder, relativePath)
        var count = 0
        val subTask = SubTask(s"匯入${relativePath}", count, fileCount.size)
        reportInfoOp.addSubTask(reportInfo._id, subTask)
        for (path <- fileCount) {
          Logger.info(s"處理 ${path.toFile.getName}")
          import com.linuxense.javadbf.DBFReader
          var reader: DBFReader = null
          var recordList = Seq.empty[NoiseRecord]
          var rowObjects: Array[Object] = null
          try {
            reader = new DBFReader(new BufferedInputStream(Files.newInputStream(path)))
            count = count + 1
            subTask.current = count
            reportInfoOp.updateSubTask(reportInfo._id, subTask)

            do {
              rowObjects = reader.nextRecord()
              if (rowObjects != null) {
                try {
                  val mntNumber = rowObjects(0).asInstanceOf[String].trim.toInt
                  val mntName = rowObjects(1).asInstanceOf[String].trim
                  val startDate = LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy-MM-dd"))
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
                    eventLdn=eventLdn, backLdn = backLdn,
                    l5 = l5, l10 = l10, l50=l50, l90 = l90, l95=l95, l99=l99, numEvent = numEvent, duration = duration)
                  recordList = recordList :+ (record)
                } catch {
                  case ex: Exception =>
                    Logger.error(s"${path.toFile.getName} 忽略第${count}筆錯誤資料")
                }
              }
            } while (rowObjects != null)
            val collection = reportRecordOp.getNoiseCollection(recordPeriod)
            val f = collection.insertMany(recordList).toFuture()
            f onFailure (errorHandler)
            Logger.info(s"成功匯入${path.toFile.getName}")
          } catch {
            case ex: Exception =>
              Logger.error(s"無法匯入 ${path.toFile.getName}", ex)
          } finally {
            DBFUtils.close(reader)
          }
        }
        self ! TaskComplete
      }
    } onFailure ({
      case ex: Exception =>
        Logger.error(s"failed to import ${relativePath}", ex)
        self ! TaskAbort(s"無法匯入${ relativePath}")
    })
  }
  import ReportRecord._
  def importDbfPhase(mainFolder: String, importTasks: Int): Receive = {

    case ImportDatabase =>
      reportInfoOp.updateState(reportInfo._id, "匯入資料庫")
      self ! ImportSecondWind
      self ! ImportSecondNoise
      self ! ImportHourlyNoise
      //self ! ImportHourlyWeather
      self ! ImportDailyNoise
      self ! ImportMonthlyNoise
      self ! ImportQuarterNoise
      self ! ImportYearlyNoise
      //self ! ImportTestNoiseEvent
      //self ! ImportNoiseEvent
      //self ! ImportDailyFlight

    case ImportSecondWind =>
      context become importDbfPhase(mainFolder, importTasks+1)
      importSecData(mainFolder, "每秒風速監測資料")

    case ImportSecondNoise =>
      context become importDbfPhase(mainFolder, importTasks+1)
      importSecData(mainFolder, "每秒噪音監測資料")

    case ImportHourlyNoise =>
      context become importDbfPhase(mainFolder, importTasks+1)
      importNoiseData(mainFolder, "每小時噪音監測資料", ReportRecord.Hour)

    case ImportDailyNoise=>
      context become importDbfPhase(mainFolder, importTasks+1)
      importNoiseData(mainFolder, "每日噪音監測資料", ReportRecord.Day)

    case ImportMonthlyNoise=>
      context become importDbfPhase(mainFolder, importTasks+1)
      importNoiseData(mainFolder, "每月噪音監測資料", ReportRecord.Month)

    case ImportQuarterNoise=>
      context become importDbfPhase(mainFolder, importTasks+1)
      importNoiseData(mainFolder, "每季噪音監測資料", ReportRecord.Quarter)

    case ImportYearlyNoise=>
      context become importDbfPhase(mainFolder, importTasks+1)
      importNoiseData(mainFolder, "一年噪音監測資料", ReportRecord.Year)

    case TaskComplete =>
      if (importTasks - 1 != 0)
        context become importDbfPhase(mainFolder, importTasks - 1)
      else {
        context become auditReportPhase(mainFolder, 0)
        self ! AuditReport
      }
    case TaskAbort(reason) =>
      reportInfo.state = "失敗"
      reportInfo.appendImportLog(reason)
      reportInfoOp.upsertReportInfo(reportInfo)
      finish(context.self.path.name)
      self ! PoisonPill
  }

  def auditReportPhase(mainFolder: String, auditTasks: Int): Receive = {
    case AuditReport =>
      finish(context.self.path.name)
      self ! PoisonPill
  }
}
