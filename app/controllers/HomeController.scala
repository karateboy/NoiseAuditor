package controllers

import akka.actor.ActorSystem
import models._
import org.apache.commons.io.file.PathUtils.createParentDirectories
import play.api._
import play.api.data.Forms._
import play.api.data._
import play.api.libs.json._
import play.api.mvc._

import java.nio.file.Paths
import javax.inject._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class HomeController @Inject()(environment: play.api.Environment, userOp: UserOp, configuration: Configuration,
                              groupOp: GroupOp, airportOp: AirportOp,
                               airportInfoOp: AirportInfoOp, actorSystem: ActorSystem, reportInfoOp: ReportInfoOp,
                               mongoDB: MongoDB) extends Controller {

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
        for (reportInfoList <- reportInfoOp.getReportInfoList(airportInfoID)) yield {
          val ver = if (reportInfoList.isEmpty)
            1
          else
            reportInfoList.map(_.version).max + 1
          val targetFilePath = Paths.get(s"${downloadFolder}/${year}Y${quarter}Q_airport${airportID}v${ver}/download.zip")
          createParentDirectories(targetFilePath)
          val file = dataFile.ref.moveTo(targetFilePath.toFile, true)
          val reportInfo = ReportInfo(airportInfoID = airportInfoID, version = ver)
          reportInfoOp.upsertReportInfo(reportInfo)
          val actorName = ReportImporter.start(dataFile = file, reportInfo = reportInfo,
            reportInfoOp, ReportRecord.getReportRecordOp(reportInfo)(mongoDB = mongoDB))(actorSystem)
          Ok(Json.obj("actorName" -> actorName, "version"->ver))
        }
      }
  }

  def getUploadProgress(actorName: String) = Security.Authenticated {
    Ok(Json.obj("finished" -> ReportImporter.isFinished(actorName)))
  }

  def getReportInfo(year:Int, quarter:Int, airportID:Int, version:Int) = Security.Authenticated.async {
    val reportID = ReportID(AirportInfoID(year, quarter, airportID), version)
    implicit val write4 = Json.writes[DataFormatError]
    implicit val write1 = Json.writes[SubTask]
    implicit val write3 = Json.writes[AirportInfoID]
    implicit val write2 = Json.writes[ReportID]
    implicit val write = Json.writes[ReportInfo]
    for(ret<- reportInfoOp.get(reportID)) yield
      Ok(Json.toJson((ret)))
  }

  case class EditData(id: String, data: String)
}
