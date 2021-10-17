package controllers

import akka.actor.ActorSystem
import models._
import org.apache.commons.io.file.PathUtils.createParentDirectories
import play.api._
import play.api.libs.json._
import play.api.mvc._

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import javax.inject._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class HomeController @Inject()(environment: play.api.Environment, userOp: UserOp, configuration: Configuration,
                               groupOp: GroupOp, airportOp: AirportOp, sysConfig: SysConfig,
                               airportInfoOp: AirportInfoOp, implicit val actorSystem: ActorSystem, reportInfoOp: ReportInfoOp,
                               mongoDB: MongoDB, auditLogOp: AuditLogOp,
                               excelUtility: ExcelUtility) extends Controller {

  val title = "機場噪音稽核系統"

  val epaReportPath: String = environment.rootPath + "/importEPA/"

  implicit val userParamRead: Reads[User] = Json.reads[User]

  import groupOp.{read, write}

  def newUser = Security.Authenticated(BodyParsers.parse.json) {
    implicit request =>
      val newUserParam = request.body.validate[User]

      newUserParam.fold(
        error => {
          Logger.error(JsError.toJson(error).toString())
          BadRequest(Json.obj("ok" -> false, "msg" -> JsError.toJson(error).toString()))
        },
        param => {
          userOp.newUser(param)
          Ok(Json.obj("ok" -> true))
        })
  }

  def deleteUser(email: String) = Security.Authenticated {
    implicit request =>
      val userInfoOpt = Security.getUserinfo(request)
      val userInfo = userInfoOpt.get

      userOp.deleteUser(email)
      Ok(Json.obj("ok" -> true))
  }

  def updateUser(id: String) = Security.Authenticated(BodyParsers.parse.json) {
    implicit request =>
      val userParam = request.body.validate[User]

      userParam.fold(
        error => {
          Logger.error(JsError.toJson(error).toString())
          BadRequest(Json.obj("ok" -> false, "msg" -> JsError.toJson(error).toString()))
        },
        param => {
          userOp.updateUser(param)
          Ok(Json.obj("ok" -> true))
        })
  }

  def getAllUsers = Security.Authenticated {
    val users = userOp.getAllUsers()
    implicit val userWrites = Json.writes[User]

    Ok(Json.toJson(users))
  }

  def newGroup = Security.Authenticated(BodyParsers.parse.json) {
    implicit request =>
      val newUserParam = request.body.validate[Group]

      newUserParam.fold(
        error => {
          Logger.error(JsError.toJson(error).toString())
          BadRequest(Json.obj("ok" -> false, "msg" -> JsError.toJson(error).toString()))
        },
        param => {
          groupOp.newGroup(param)
          Ok(Json.obj("ok" -> true))
        })
  }

  def deleteGroup(id: String) = Security.Authenticated {
    implicit request =>
      val ret = groupOp.deleteGroup(id)
      Ok(Json.obj("ok" -> (ret.getDeletedCount != 0)))
  }

  def updateGroup(id: String) = Security.Authenticated(BodyParsers.parse.json) {
    implicit request =>
      val userParam = request.body.validate[Group]

      userParam.fold(
        error => {
          Logger.error(JsError.toJson(error).toString())
          BadRequest(Json.obj("ok" -> false, "msg" -> JsError.toJson(error).toString()))
        },
        param => {
          val ret = groupOp.updateGroup(param)
          Ok(Json.obj("ok" -> (ret.getMatchedCount != 0)))
        })
  }

  def getAllGroups = Security.Authenticated {
    val groups = groupOp.getAllGroups()

    Ok(Json.toJson(groups))
  }

  def getUser(id: String) = Security.Authenticated {
    implicit request =>
      implicit val write = Json.writes[User]
      val user = userOp.getUserByEmail(id)
      Ok(Json.toJson(user))
  }

  def getAirportList() = Security.Authenticated.async {
    implicit val writes = Json.writes[Airport]
    for (ret <- airportOp.getList()) yield {
      Ok(Json.toJson(ret))
    }
  }

  def upsertAirportInfo() = Security.Authenticated.async(BodyParsers.parse.json) {
    implicit request =>
      import airportInfoOp._
      val airportRet = request.body.validate[AirportInfo]

      airportRet.fold(
        error => {
          Future {
            Logger.error(JsError.toJson(error).toString())
            BadRequest(Json.obj("ok" -> false, "msg" -> JsError.toJson(error).toString()))
          }
        },
        airportInfo => {
          for (ret <- airportInfoOp.upsert(airportInfo)) yield
            Ok(Json.obj("ok" -> ret.wasAcknowledged()))
        })
  }

  def getLatestAirportInfo(airportID: Int, year: Int, quarter: Int) = Security.Authenticated.async {
    import airportInfoOp._
    val f = airportInfoOp.getLatest(airportID)
    for (ret <- f) yield {
      if (ret.nonEmpty) {
        val airportInfo = ret(0)
        val matched = airportInfo._id.year == year && airportInfo._id.quarter == quarter
        Ok(Json.obj("ok" -> matched, "result" -> Json.toJson(ret(0))))
      } else
        Ok(Json.obj("ok" -> false))
    }
  }

  def getAirportInfo(airportID: Int, year: Int, quarter: Int) = Security.Authenticated.async {
    import airportInfoOp._
    val f = airportInfoOp.get(airportID, year, quarter)
    for (ret <- f) yield {
      if (ret.nonEmpty) {
        val airportInfo = ret(0)
        val matched = airportInfo._id.year == year && airportInfo._id.quarter == quarter
        Ok(Json.obj("ok" -> matched, "result" -> Json.toJson(ret(0))))
      } else
        Ok(Json.obj("ok" -> false))
    }
  }

  def uploadeReport(airportID: Int, year: Int, quarter: Int) = Security.Authenticated.async(parse.multipartFormData) {
    implicit request =>
      val dataFileOpt = request.body.file("data")
      if (dataFileOpt.isEmpty) {
        Future {
          Logger.info("data is empty..")
          Ok(Json.obj("ok" -> true))
        }
      } else {
        val config: Configuration = configuration.getConfig("auditor").get
        val downloadFolder = config.getString("downloadFolder").get
        val dataFile = dataFileOpt.get
        val airportInfoID = AirportInfoID(year, quarter, airportID)
        for {reportInfoList <- reportInfoOp.getReportInfoList(airportInfoID)
             reportTolerance <- ReportTolerance.get(sysConfig)
             } yield {
          val ver = if (reportInfoList.isEmpty)
            1
          else
            reportInfoList.map(_.version).max + 1
          val targetFilePath = Paths.get(s"${downloadFolder}/${year}Y${quarter}Q_airport${airportID}v${ver}/download.zip")
          createParentDirectories(targetFilePath)
          val file = dataFile.ref.moveTo(targetFilePath.toFile, true)
          val reportInfo = ReportInfo(airportInfoID = airportInfoID, version = ver)
          reportInfoOp.upsertReportInfo(reportInfo)
          val actorName = ReportImporter.start(dataFile = file, airportInfoOp = airportInfoOp, reportInfo = reportInfo,
            reportInfoOp, ReportRecord.getReportRecordOp(reportInfo)(mongoDB = mongoDB),
            reportTolerance, auditLogOp)
          Ok(Json.obj("actorName" -> actorName, "version" -> ver))
        }
      }
  }

  def getUploadProgress(actorName: String) = Security.Authenticated {
    Ok(Json.obj("finished" -> ReportImporter.isFinished(actorName)))
  }

  def getReportInfoIdList() = Security.Authenticated.async {
    val f = reportInfoOp.getAllReortInfoList()
    for (ret <- f) yield {
      implicit val write3 = Json.writes[AirportInfoID]
      implicit val write2 = Json.writes[ReportID]
      val idList = ret.map { r => r._id }
      Ok(Json.toJson(idList))
    }

  }

  def getReportInfo(year: Int, quarter: Int, airportID: Int, version: Int) = Security.Authenticated.async {
    val reportID = ReportID(AirportInfoID(year, quarter, airportID), version)
    implicit val write4 = Json.writes[DataFormatError]
    implicit val write1 = Json.writes[SubTask]
    implicit val write3 = Json.writes[AirportInfoID]
    implicit val write2 = Json.writes[ReportID]
    implicit val write = Json.writes[ReportInfo]
    for (ret <- reportInfoOp.get(reportID)) yield
      Ok(Json.toJson((ret)))
  }

  def reauditReport() = Security.Authenticated.async(BodyParsers.parse.json) {
    implicit request =>
      implicit val r1 = Json.reads[AirportInfoID]
      implicit val read = Json.reads[ReportID]
      val ret = request.body.validate[ReportID]

      ret.fold(

        error => {
          Future {
            Logger.error(JsError.toJson(error).toString())
            BadRequest(Json.obj("ok" -> false, "msg" -> JsError.toJson(error).toString()))
          }
        },
        id => {
          for {reportInfo <- reportInfoOp.get(id)
               reportTolerance <- ReportTolerance.get(sysConfig)
               } yield {
            if (reportInfo.nonEmpty) {
              ReportImporter.reaudit(airportInfoOp, reportInfo(0),
                reportInfoOp, ReportRecord.getReportRecordOp(reportInfo(0))(mongoDB = mongoDB),
                reportTolerance, auditLogOp)

              Ok(Json.obj("ok" -> true))
            } else
              NotAcceptable("No such report!")
          }

        })
  }

  def clearAllReport() = Security.Authenticated {
    reportInfoOp.clearAll()
    Ok("clear all reportInfo related collections")
  }

  def getAuditReport(year: Int, quarter: Int, airportID: Int, version: Int) = Security.Authenticated.async {
    val config: Configuration = configuration.getConfig("auditor").get
    val downloadFolder = config.getString("downloadFolder").get
    val targetFolderPath = Paths.get(s"${downloadFolder}/Report${year}Y${quarter}Q_airport${airportID}v${version}/")
    Files.createDirectories(targetFolderPath)
    val airportF = airportInfoOp.get(airportID, year, quarter)
    var terminalMap = Map.empty[Int, Terminal]
    val auditLogFF =
      for (ret <- airportF) yield {
        val info = ret(0)
        terminalMap = info.map()
        val listF =
          for (mntNum <- terminalMap.keys.toList) yield
            auditLogOp.get(AuditLogID(ReportID(AirportInfoID(year, quarter, airportID), version), mntNum))

        Future.sequence(listF)
      }

    val retF = auditLogFF.flatMap(x => x)
    for (ret <- retF) yield {
      val map = terminalMap.map(p=>p._1->p._2.name).toMap
      val files: Seq[File] =
        for (log <- ret) yield
          excelUtility.getAuditReports(log, map)

      for(file<-files) {
        val p = Paths.get(targetFolderPath+(s"/${file.getName}"))
        Files.move(file.toPath, p, StandardCopyOption.REPLACE_EXISTING)
      }

      val cmd = """"C:/Program Files/7-Zip/7z.exe" a report.zip """ + targetFolderPath.toFile.getAbsolutePath
      val process = Runtime.getRuntime.exec(cmd,
        Array.empty[String], targetFolderPath.getParent.toFile)

      process.waitFor()
      if (process.exitValue() != 0) {
        BadRequest("Failed")
      }else{
        val targetFilePath: String = targetFolderPath.getParent+("/report.zip")
        Ok.sendFile(new File(targetFilePath), fileName = _ =>
          play.utils.UriEncoding.encodePathSegment("report.zip", "UTF-8"),
          onClose = () => {
            Files.deleteIfExists(new File(targetFilePath).toPath)
          })
      }
    }
  }

  case class EditData(id: String, data: String)
}
