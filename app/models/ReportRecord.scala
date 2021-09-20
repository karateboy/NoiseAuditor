package models

import org.mongodb.scala.MongoCollection
import org.mongodb.scala.bson.ObjectId

import java.util.Date
import scala.concurrent.Future

object ReportRecord {
  def getReportRecordOp(reportInfo: ReportInfo)(mongoDB: MongoDB) = {
    val op = ReportRecordOp(reportInfo, mongoDB)
    op.init
    op
  }

  def getRecordTypeName(recordType: RecordType) = recordType.toString

  def getRecordTypeByName(name: String): RecordType =
    name match {
      case "Noise" =>
        Noise
      case "WindSpeed" =>
        WindSpeed
      case "WindDirection" =>
        WindDirection
      case "Event" =>
        Event
      case _ =>
        throw new Exception(s"Unknown $name RecordType")
    }

  sealed trait RecordType

  sealed trait RecordPeriod

  case object Noise extends RecordType

  case object WindSpeed extends RecordType

  case object WindDirection extends RecordType

  case object Event extends RecordType

  case object Min extends RecordPeriod

  case object Hour extends RecordPeriod

  case object Day extends RecordPeriod

  case object Month extends RecordPeriod

  case object Quarter extends RecordPeriod

  case object Year extends RecordPeriod
}

case class SecRecord(time: Date, value: Double)

case class RecordID(time: Date, terminalID: Int, recordType: String)

case class MinRecord(_id: RecordID, records: Seq[SecRecord])

case class WindHourRecord(_id: RecordID, windAvg: Double, windMax: Double)

case class NoiseRecord(_id: RecordID, activity: Int, totalEvent: Double, totalLeq: Double, eventLeq: Double, backLeq: Double,
                       totalLdn: Double, eventLdn: Double, backLdn: Double, l5: Double, l10: Double, l50: Double,
                       l90: Double, l95: Double, l99: Double, numEvent: Int, duration: Int)

case class EventRecord(_id: RecordID, duration: Int, setl: Double, minDur: Double, eventLeg: Double, eventSel: Double,
                       eventMaxLen: Double, eventMaxTime: Int, records: Seq[SecRecord])

case class FlightInfo(_id: ObjectId, startDate: Date, startTime: Date, acftID: String, operation: String, runway: String, flightRoute: String)

object FlightInfo {
  def apply(startDate: Date, startTime: Date, acftID: String, operation: String, runway: String, flightRoute: String): FlightInfo =
    FlightInfo(new ObjectId(), startDate, startTime, acftID, operation, runway, flightRoute)
}

case class ReportRecordOp(reportInfo: ReportInfo, mongoDB: MongoDB) {

  import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
  import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
  import org.mongodb.scala.bson.codecs.Macros._

  val minCodecRegistry = fromRegistries(fromProviders(classOf[MinRecord], classOf[RecordID], classOf[SecRecord]), DEFAULT_CODEC_REGISTRY)
  val windHrCodeRegistry = fromRegistries(fromProviders(classOf[WindHourRecord], classOf[RecordID]), DEFAULT_CODEC_REGISTRY)
  val noiseCodeRegistry = fromRegistries(fromProviders(classOf[NoiseRecord], classOf[RecordID]), DEFAULT_CODEC_REGISTRY)
  val eventCodeRegistry = fromRegistries(fromProviders(classOf[EventRecord], classOf[RecordID], classOf[SecRecord]), DEFAULT_CODEC_REGISTRY)
  val flightInfoCodeRegistry = fromRegistries(fromProviders(classOf[FlightInfo]), DEFAULT_CODEC_REGISTRY)

  import ReportRecord._

  def init(): Future[Void] = {
    mongoDB.database.createCollection(s"${reportInfo.getCollectionName}Min").toFuture()
    mongoDB.database.createCollection(s"${reportInfo.getCollectionName}Hr").toFuture()
    mongoDB.database.createCollection(s"${reportInfo.getCollectionName}Day").toFuture()
    mongoDB.database.createCollection(s"${reportInfo.getCollectionName}Month").toFuture()
    mongoDB.database.createCollection(s"${reportInfo.getCollectionName}Quarter").toFuture()
    mongoDB.database.createCollection(s"${reportInfo.getCollectionName}Year").toFuture()
    mongoDB.database.createCollection(s"${reportInfo.getCollectionName}WindMin").toFuture()
    mongoDB.database.createCollection(s"${reportInfo.getCollectionName}WindHr").toFuture()
    mongoDB.database.createCollection(s"${reportInfo.getCollectionName}TestEvent").toFuture()
    mongoDB.database.createCollection(s"${reportInfo.getCollectionName}Event").toFuture()
    mongoDB.database.createCollection(s"${reportInfo.getCollectionName}Flight").toFuture()
  }

  def getNoiseCollection(recordPeriod: RecordPeriod): MongoCollection[NoiseRecord] = {
    recordPeriod match {
      case Min =>
        throw new Exception("shall use other function!")
      case Hour =>
        mongoDB.database.withCodecRegistry(noiseCodeRegistry).getCollection[NoiseRecord](s"${reportInfo.getCollectionName}Hr")
      case Day =>
        mongoDB.database.withCodecRegistry(noiseCodeRegistry).getCollection[NoiseRecord](s"${reportInfo.getCollectionName}Day")
      case Month =>
        mongoDB.database.withCodecRegistry(noiseCodeRegistry).getCollection[NoiseRecord](s"${reportInfo.getCollectionName}Month")
      case Quarter =>
        mongoDB.database.withCodecRegistry(noiseCodeRegistry).getCollection[NoiseRecord](s"${reportInfo.getCollectionName}Quarter")
      case Year =>
        mongoDB.database.withCodecRegistry(noiseCodeRegistry).getCollection[NoiseRecord](s"${reportInfo.getCollectionName}Year")
    }
  }

  def getMinCollection(recordType: RecordType): MongoCollection[MinRecord] = {
    recordType match {
      case Noise =>
        mongoDB.database.withCodecRegistry(minCodecRegistry).getCollection[MinRecord](s"${reportInfo.getCollectionName}Min")
      case Event =>
        throw new Exception("No event min collection type!")
      case _ =>
        mongoDB.database.withCodecRegistry(minCodecRegistry).getCollection[MinRecord](s"${reportInfo.getCollectionName}WindMin")
    }
  }

  def getWindHourCollection: MongoCollection[WindHourRecord] =
    mongoDB.database.withCodecRegistry(windHrCodeRegistry).getCollection[WindHourRecord](s"${reportInfo.getCollectionName}WindHr")

  def getEventCollection: MongoCollection[EventRecord] =
    mongoDB.database.withCodecRegistry(eventCodeRegistry).getCollection[EventRecord](s"${reportInfo.getCollectionName}Event")

  def getTestEventCollection: MongoCollection[EventRecord] =
    mongoDB.database.withCodecRegistry(eventCodeRegistry).getCollection[EventRecord](s"${reportInfo.getCollectionName}TestEvent")

  def getFlightCollection: MongoCollection[FlightInfo] =
    mongoDB.database.withCodecRegistry(flightInfoCodeRegistry).getCollection[FlightInfo](s"${reportInfo.getCollectionName}Flight")
}
