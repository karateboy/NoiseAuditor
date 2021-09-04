package models

import models.ModelHelper.errorHandler

import javax.inject.{Inject, Singleton}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class Terminal(no:Int, name:String)
case class ReportID(year:Int, quarter:Int, airpotID:Int, version:Int)
case class AirportInfo(_id:Int, terminals: Seq[Terminal])
case class ReportInfo(_id:ReportID, year: Int, quarter:Int, airportInfo:AirportInfo, version:Int,
                      state:String = "未進行"){
  def getCollectionName = s"Y${year}Q${quarter}airport${airportInfo._id}v${version}"
}

object ReportInfo{
  def apply(year: Int, quarter:Int, airportInfo: AirportInfo, version:Int): ReportInfo =
    ReportInfo(ReportID(year, quarter, airportInfo._id, version), year, quarter, airportInfo, version)
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
    classOf[ReportID], classOf[Airport], classOf[Terminal]), DEFAULT_CODEC_REGISTRY)
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