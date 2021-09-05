package models

import models.ModelHelper.errorHandler

import javax.inject.{Inject, Singleton}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class ReportID(airpotInfoID:AirportInfoID, version:Int)
case class ReportInfo(_id:ReportID, year: Int, quarter:Int, airportInfo:AirportInfo, version:Int = 0,
                      state:String = "未進行"){
  def getCollectionName = s"Y${year}Q${quarter}airport${airportInfo._id.airportID}v${version}"
}

object ReportInfo{
  def apply(airportInfo: AirportInfo, version:Int): ReportInfo =
    ReportInfo(ReportID(airportInfo._id, version), airportInfo._id.year, airportInfo._id.quarter, airportInfo, version)
}

@Singleton
class ReportInfoOp @Inject()(mongoDB: MongoDB) {
  import org.mongodb.scala._
  import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
  import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
  import org.mongodb.scala.bson.codecs.Macros._
  import org.mongodb.scala.model._

  val ColName = "reportInfos"
  val codecRegistry = fromRegistries(fromProviders(classOf[ReportInfo],
    classOf[ReportID], classOf[AirportInfoID], classOf[AirportInfo], classOf[Terminal]), DEFAULT_CODEC_REGISTRY)
  val collection: MongoCollection[ReportInfo] = mongoDB.database.withCodecRegistry(codecRegistry).getCollection(ColName)

  collection.createIndex(Indexes.ascending("airportInfo._id", "year", "quarter"), IndexOptions().unique(true))

  def getReportInfoList(airportID:Int): Future[Seq[ReportInfo]] ={
    val f = collection.find(Filters.equal("airportID", airportID)).toFuture()
    f onFailure(errorHandler)
    f
  }

  def upsertReportInfo(reportInfo: ReportInfo) = {
    val f = collection.replaceOne(Filters.equal("_id", reportInfo._id), reportInfo,
      ReplaceOptions().upsert(true)).toFuture()
    f onFailure(errorHandler)
    f
  }
}