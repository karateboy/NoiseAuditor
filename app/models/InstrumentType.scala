package models

import akka.actor.{ActorContext, ActorRef}
import com.github.nscala_time.time.Imports._
import play.api.Logger
import play.api.libs.concurrent.InjectedActorSupport
import play.api.libs.json._

case class ProtocolInfo(id: Protocol.Value, desp: String)

case class InstrumentTypeInfo(id: String, desp: String, protocolInfo: List[ProtocolInfo])

case class InstrumentType(id: String, desp: String, protocol: List[Protocol.Value],
                          driver: DriverOps, diFactory: AnyRef, analog: Boolean)

object InstrumentType {
  def apply(driver: DriverOps, diFactory: AnyRef, analog: Boolean = false): InstrumentType =
    InstrumentType(driver.id, driver.description, driver.protocol, driver, diFactory, analog)
}

trait DriverOps {

  import Protocol.ProtocolParam
  import akka.actor._

  def id: String
  def description: String
  def protocol: List[Protocol.Value]

  def verifyParam(param: String): String

  def getMonitorTypes(param: String): List[String]

  def getCalibrationTime(param: String): Option[LocalTime]

  def factory(id: String, protocol: ProtocolParam, param: String)(f: AnyRef): Actor

  def isDoInstrument: Boolean = false

  def isCalibrator:Boolean = false
}

import javax.inject._

@Singleton
class InstrumentTypeOp @Inject()
() extends InjectedActorSupport {

  import Protocol._

  implicit val prtocolWrite = Json.writes[ProtocolInfo]
  implicit val write = Json.writes[InstrumentTypeInfo]

  val otherDeviceList = Seq.empty[InstrumentType]

  val otherMap = otherDeviceList.map(dt=> dt.id->dt).toMap
  val map: Map[String, InstrumentType] = otherMap

  val DoInstruments = otherDeviceList.filter(_.driver.isDoInstrument)
  var count = 0

  def getInstInfoPair(instType: InstrumentType) = {
    instType.id -> instType
  }

  def start(instType: String, id: String, protocol: ProtocolParam, param: String)(implicit context: ActorContext): ActorRef = {
    val actorName = s"${instType}_${count}"
    Logger.info(s"$actorName is created.")
    count += 1

    val instrumentType = map(instType)
    injectedChild(instrumentType.driver.factory(id, protocol, param)(instrumentType.diFactory), actorName)
  }
}

