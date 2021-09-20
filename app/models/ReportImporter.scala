package models

import akka.actor._
import com.linuxense.javadbf.DBFUtils
import com.mongodb.client.model.{InsertOneModel, UpdateManyModel}
import models.ReportRecord.{WindDirection, WindSpeed}
import org.joda.time.{DateTime, LocalDate, LocalTime}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.mongodb.scala.model
import org.mongodb.scala.model.UpdateManyModel
import play.api._
import ModelHelper._
import org.apache.commons.io.FileUtils

import java.io.{BufferedInputStream, File, FileInputStream}
import java.nio.file.{Files, Path, Paths}
import java.util.Date
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, blocking}

object ReportImporter {
  var n = 0
  private var actorRefMap = Map.empty[String, ActorRef]

  def start(dataFile: File, reportInfo: ReportInfo, reportInfoOp: ReportInfoOp, reportRecordOp:ReportRecordOp)(implicit actorSystem: ActorSystem) = {
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

  def props(dataFile: File, reportInfo: ReportInfo, reportInfoOp: ReportInfoOp, reportRecordOp:ReportRecordOp) =
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
  import reportRecordOp._

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

  def getDbfList(mainFolder: String, relativePath:String): List[Path] = {
    val paths: List[Path] = Files.list(Paths.get(mainFolder, relativePath)).iterator().asScala.toList
    val ret = paths.filter(!Files.isDirectory(_))
    Logger.info(s"$mainFolder\\$relativePath #=${ret.size}")
    ret
  }

  def importDbfPhase(mainFolder: String, importTasks: Int): Receive = {
    case ImportDatabase =>
      self ! ImportSecondWind
      //self ! ImportSecondNoise
      //self ! ImportHourlyNoise
      //self ! ImportHourlyWeather
      //self ! ImportDailyNoise
      //self ! ImportMonthlyNoise
      //self ! ImportQuarterNoise
      //self ! ImportYearlyNoise
      //self ! ImportTestNoiseEvent
      //self ! ImportNoiseEvent
      //self ! ImportDailyFlight
      context become importDbfPhase(mainFolder, 1)

    case ImportSecondWind =>
      Logger.debug("ImportSecondWind")
      Future {
        blocking {
            for (path <- getDbfList(mainFolder, "每秒風速監測資料")) yield {
              val recordType =
                if (path.toFile.getName.contains("wd"))
                  WindDirection
                else
                  WindSpeed
              Logger.info(s"處理 ${path.toFile.getName}")
              import com.linuxense.javadbf.DBFReader
              var reader: DBFReader = null
              var recordList = Seq.empty[MinRecord]
              var rowObjects: Array[Object] = null
              try {
                reader = new DBFReader(new BufferedInputStream(Files.newInputStream(path)))
                var count = 0
                do{
                  rowObjects = reader.nextRecord()
                  count = count + 1
                  if(rowObjects != null){
                    try{
                      val mntNumber = rowObjects(0).asInstanceOf[String].trim.toInt
                      val mntName = rowObjects(1).asInstanceOf[String].trim
                      val startDate = LocalDate.parse(rowObjects(2).asInstanceOf[String].trim, DateTimeFormat.forPattern("yyyy-MM-dd"))
                      val startTime = LocalTime.parse(rowObjects(3).asInstanceOf[String].trim, DateTimeFormat.forPattern("HH:mm"))
                      val start = startDate.toLocalDateTime(startTime)
                      var secRecords = Seq.empty[SecRecord]
                      for(i <- 0 to 59) {
                        try{
                          secRecords = secRecords:+SecRecord(start.plusSeconds(i).toDate(), rowObjects(i+5).asInstanceOf[java.math.BigDecimal].doubleValue())
                        }catch{
                          case ex:Exception=>
                            Logger.error(s"${path.toFile.getName} ${mntName} ${start} 遺失第${i}秒資料")
                        }
                      }
                      val record = MinRecord(RecordID(start.toDate, mntNumber, recordType.toString), secRecords)
                      recordList = recordList:+(record)
                    }catch{
                      case ex:Exception=>
                        Logger.error(s"${path.toFile.getName} 忽略第${count}筆錯誤資料")
                    }
                  }
                }while(rowObjects != null)
                val collection = reportRecordOp.getMinCollection(recordType)
                val f = collection.insertMany(recordList).toFuture()
                f onFailure(errorHandler)
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
          Logger.error("failed to importSecondWind", ex)
          self ! TaskAbort("無法匯入每秒氣象資料")
      })

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
