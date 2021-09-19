package models

import akka.actor._
import play.api._

import java.io.File
import javax.inject.Inject
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, blocking}

object ReportImporter {
  var n = 0
  private var actorRefMap = Map.empty[String, ActorRef]

  def start(dataFile: File, reportInfo: ReportInfo, reportInfoOp: ReportInfoOp)(implicit actorSystem: ActorSystem) = {
    val name = getName
    val actorRef = actorSystem.actorOf(ReportImporter.props(dataFile = dataFile, reportInfo=reportInfo, reportInfoOp), name)
    actorRefMap = actorRefMap + (name -> actorRef)
    name
  }

  def getName = {
    n = n + 1
    s"dataImporter${n}"
  }

  def props(dataFile: File, reportInfo: ReportInfo, reportInfoOp: ReportInfoOp) =
    Props(classOf[ReportImporter], dataFile, reportInfo, reportInfoOp)

  def finish(actorName: String) = {
    actorRefMap = actorRefMap.filter(p => {
      p._1 != actorName
    })
  }

  def isFinished(actorName: String) = {
    !actorRefMap.contains(actorName)
  }

  sealed trait FileType

  case object Import

  case object ImportDatabase

  case object Complete
}

class ReportImporter(dataFile: File, reportInfo: ReportInfo, reportInfoOp: ReportInfoOp) extends Actor {

  import ReportImporter._

  self ! Import

  override def receive: Receive = {
    case Import =>
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
              self ! Complete
            }else{
              val message = "解壓縮成功"
              reportInfo.state = message
              reportInfo.appendImportLog(message)
              reportInfoOp.upsertReportInfo(reportInfo)
              dataFile.delete()
              self ! ImportDatabase
            }
          } catch {
            case ex: Exception =>
              Logger.error("failed to import", ex)
              self ! Complete
          }
        }
      }
    case ImportDatabase =>
      self ! Complete

    case Complete =>
      finish(context.self.path.name)
      self ! PoisonPill
  }
}
