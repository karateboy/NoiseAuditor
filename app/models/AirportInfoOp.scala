package models

import models.ModelHelper.errorHandler
import org.mongodb.scala.result.UpdateResult
import play.api.Logger
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext.Implicits.global
import javax.inject.{Inject, Singleton}
import scala.concurrent.Future
import scala.util.{Failure, Success}

case class Terminal(no:Int, name:String)
case class AirportInfoID(year:Int, quarter:Int, airportID:Int)
case class AirportInfo(_id:AirportInfoID, terminals: Seq[Terminal])

@Singleton
class AirportInfoOp @Inject()(mongoDB: MongoDB){
  import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
  import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
  import org.mongodb.scala._
  import org.mongodb.scala.bson.codecs.Macros._
  import org.mongodb.scala.model._

  implicit val read2 = Json.reads[Terminal]
  implicit val read1 = Json.reads[AirportInfoID]
  implicit val reads = Json.reads[AirportInfo]
  implicit val write2 = Json.writes[Terminal]
  implicit val write1 = Json.writes[AirportInfoID]
  implicit val writes = Json.writes[AirportInfo]

  val ColName = "airportInfos"
  val codecRegistry = fromRegistries(fromProviders(classOf[AirportInfo], classOf[AirportInfoID], classOf[Terminal]), DEFAULT_CODEC_REGISTRY)
  val collection: MongoCollection[AirportInfo] = mongoDB.database.withCodecRegistry(codecRegistry).getCollection(ColName)

  def init(): Unit = {
    for(colNames <- mongoDB.database.listCollectionNames().toFuture()) {
      if (!colNames.contains(ColName)) {
        val f = mongoDB.database.createCollection(ColName).toFuture()
        f.onFailure(errorHandler)
        f onComplete({
          case Success(_) =>
          case Failure(ex)=>
            Logger.error("failed to init airportInfos collection", ex)
        })
      }
    }
  }

  init

  def get(airportID:Int, year:Int, quarter:Int): Future[Seq[AirportInfo]] ={
    val filter = Filters.and(Filters.equal("_id.year", year),
      Filters.equal("_id.quarter", quarter),
      Filters.equal("_id.airportID", airportID))
    val f = collection.find(filter).toFuture()
    f onFailure(errorHandler())
    f
  }

  def getLatest(airportID:Int): Future[Seq[AirportInfo]] ={
    val filter = Filters.equal("_id.airportID", airportID)
    val f = collection.find(filter).sort(Sorts.descending("_id.year", "_id.quarter")).toFuture()
    f onFailure(errorHandler())
    f
  }

  def upsert(airportInfo: AirportInfo): Future[UpdateResult] = {
    val f = collection.replaceOne(Filters.equal("_id", airportInfo._id), airportInfo, ReplaceOptions()
      .upsert(true)).toFuture()
    f onFailure(errorHandler())
    f
  }
}
