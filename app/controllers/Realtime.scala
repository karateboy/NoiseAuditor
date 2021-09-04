package controllers

import com.github.nscala_time.time.Imports._
import models._
import play.api.libs.json._
import play.api.mvc._

import javax.inject._
import scala.concurrent.ExecutionContext.Implicits.global

class Realtime @Inject()
(monitorTypeOp: MonitorTypeOp, dataCollectManagerOp: DataCollectManagerOp, instrumentOp: InstrumentOp,
 monitorStatusOp: MonitorStatusOp) extends Controller {
  val overTimeLimit = 6

  case class MonitorTypeStatus(_id: String, desp: String, value: String, unit: String, instrument: String, status: String, classStr: Seq[String], order: Int)

  def MonitorTypeStatusList() = Security.Authenticated.async {
    implicit request =>

      implicit val mtsWrite = Json.writes[MonitorTypeStatus]

      val result =
        for {
          instrumentMap <- instrumentOp.getInstrumentMap()
          dataMap <- dataCollectManagerOp.getLatestData()
        } yield {
          val list =
            for {
              mt <- monitorTypeOp.realtimeMtvList
              recordOpt = dataMap.get(mt)
            } yield {
              val mCase = monitorTypeOp.map(mt)
              val measuringByStr = mCase.measuringBy.map {
                instrumentList =>
                  instrumentList.mkString(",")
              }.getOrElse("??")

              if (recordOpt.isDefined) {
                val record = recordOpt.get
                val duration = new Duration(record.time, DateTime.now())
                val (overInternal, overLaw) = monitorTypeOp.overStd(mt, record.value)
                val status = if (duration.getStandardSeconds <= overTimeLimit)
                  monitorStatusOp.map(record.status).desp
                else {
                  if (mCase.measuringBy.isDefined) {
                    val instruments: Seq[Instrument] = mCase.measuringBy.get map {
                      instrumentMap
                    }
                    if (instruments.exists(inst => !inst.active))
                      "停用"
                    else
                      "斷線"
                  } else
                    "斷線"
                }

                MonitorTypeStatus(_id = mCase._id, desp = mCase.desp, monitorTypeOp.format(mt, Some(record.value)), mCase.unit, measuringByStr,
                  status,
                  MonitorStatus.getCssClassStr(record.status, overInternal, overLaw), mCase.order)
              } else {
                val status = if (mCase.measuringBy.isDefined) {
                  val instruments: Seq[Instrument] = mCase.measuringBy.get map {
                    instrumentMap
                  }
                  if (instruments.exists(inst => !inst.active))
                    "停用"
                  else
                    "斷線"
                } else
                  "斷線"

                MonitorTypeStatus(_id = mCase._id, mCase.desp, monitorTypeOp.format(mt, None), mCase.unit, measuringByStr,
                  status,
                  Seq("abnormal_status"), mCase.order)
              }
            }
          Ok(Json.toJson(list))
        }

      result
  }
}