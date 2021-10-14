package models

import play.api.libs.json.Json

case class ReportTolerance(totalLeq: Double, eventLeg: Double, backLeq: Double,
                           totalLdn: Double, eventLdn: Double, backLdn: Double,
                           totalEventSel: Double, numOfEvent: Int, duration: Int)

import scala.concurrent.ExecutionContext.Implicits.global
object ReportTolerance {
  val default = ReportTolerance(1.0, 1.0, 1.0,
    1.0, 1.0, 1.0, 1.0, 2, 2)

  implicit val write = Json.writes[ReportTolerance]
  implicit val read = Json.reads[ReportTolerance]

  def get(implicit sysConfig:SysConfig) = {
    for(v<-sysConfig.get(sysConfig.ReportToleranceKey))yield {
      val json = v.asString().getValue
      Json.parse(json).validate[ReportTolerance].getOrElse(default)
    }
  }
}
