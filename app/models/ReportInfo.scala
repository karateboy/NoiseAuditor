package models

import models.ModelHelper.errorHandler
import org.joda.time.DateTime
import play.api.Logger

import javax.inject.{Inject, Singleton}
import scala.collection.script.Update
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}
import org.mongodb.scala.model._
import org.mongodb.scala.result.UpdateResult

case class SubTask(name:String, var current:Int, total:Int)
case class ReportID(airpotInfoID:AirportInfoID, version:Int)
case class ReportInfo(_id:ReportID, year: Int, quarter:Int, version:Int = 0,
                      var state:String = "上傳檔案中",
                      var unableAuditReason: Seq[String] = Seq.empty[String],
                      reportTolerance: Option[ReportTolerance] = None,
                      tasks: Seq[SubTask] = Seq.empty[SubTask]){
  val getCollectionName = s"Y${year}Q${quarter}airport${_id.airpotInfoID.airportID}v${_id.version}"

  def removeCollection(mongoDB: MongoDB) = {
    for(nameList <- mongoDB.database.listCollectionNames().toFuture()){
      for(name <-nameList){
        if(name.startsWith(getCollectionName))
          mongoDB.database.getCollection(name).drop().toFuture()
      }
    }
  }

  def appendUnableAuditReason(reason:String): Unit ={
    unableAuditReason = unableAuditReason :+ reason
  }
}

object ReportInfo{
  def apply(airportInfoID: AirportInfoID, version:Int): ReportInfo =
    ReportInfo(ReportID(airportInfoID, version), airportInfoID.year, airportInfoID.quarter, version)
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
    classOf[ReportID], classOf[AirportInfoID], classOf[AirportInfo], classOf[Terminal], classOf[SubTask],
    classOf[ReportTolerance]), DEFAULT_CODEC_REGISTRY)
  val collection: MongoCollection[ReportInfo] = mongoDB.database.withCodecRegistry(codecRegistry).getCollection(ColName)

  collection.createIndex(Indexes.ascending("airportInfo._id", "year", "quarter"), IndexOptions().unique(true))

  def init(): Unit = {
    for(colNames <- mongoDB.database.listCollectionNames().toFuture()) {
      if (!colNames.contains(ColName)) {
        val f = mongoDB.database.createCollection(ColName).toFuture()
        f.onFailure(errorHandler)
        f onComplete({
          case Success(_) =>
          case Failure(ex)=>
            Logger.error(s"failed to init $ColName collection", ex)
        })
      }
    }
  }

  init

  def getAllReortInfoList(): Future[Seq[ReportInfo]] = {
    val f = collection.find(Filters.exists("_id")).toFuture()
    f onFailure(errorHandler)
    f
  }
  def getReportInfoList(airpotInfoID:AirportInfoID): Future[Seq[ReportInfo]] ={
    val f = collection.find(Filters.equal("_id.airpotInfoID", airpotInfoID)).toFuture()
    f onFailure(errorHandler)
    f
  }

  def upsertReportInfo(reportInfo: ReportInfo) = {
    val f = collection.replaceOne(Filters.equal("_id", reportInfo._id), reportInfo,
      ReplaceOptions().upsert(true)).toFuture()
    f onFailure(errorHandler)
    f
  }

  def updateState(reportID:ReportID, state:String)={
    val f = collection.updateOne(Filters.equal("_id", reportID), Updates.set("state", state)).toFuture()
    f onFailure(errorHandler)
    f
  }

  def clearAll()={
    val f = collection.find(Filters.exists("_id")).toFuture()
    for(ret<-f){
      ret foreach { r => r.removeCollection(mongoDB)}
    }
  }

  def addSubTask(_id:ReportID, task:SubTask): Future[UpdateResult] = {
    val updates = Updates.addToSet("tasks", task)
    val f = collection.updateOne(Filters.equal("_id", _id), updates).toFuture()
    f onFailure(errorHandler())
    f
  }

  def incSubTaskCurrentCount(_id:ReportID, taskName:String) = {
    val filter = Filters.and(Filters.equal("_id", _id),
      Filters.equal("tasks.name", taskName))
    val update = Updates.inc("tasks.$.current", 1)
    val f = collection.updateOne(filter, update).toFuture()
    f onFailure(errorHandler())
    f
  }

  def setReortTolerance(_id:ReportID, rt:ReportTolerance) = {
    val update = Updates.set("reportTolerance", rt)
    val f = collection.updateOne(Filters.equal("_id", _id), update).toFuture()
    f onFailure(errorHandler())
    f
  }

  def get(_id:ReportID): Future[Seq[ReportInfo]] ={
    val f = collection.find(Filters.equal("_id", _id)).toFuture()
    f onFailure(errorHandler())
    f
  }

  def removeSubTask(_id:ReportID, taskName:String) = {
    val filter = Filters.and(Filters.equal("_id", _id))

    val update = Updates.pullByFilter(Document("tasks"->Document("name"->taskName)))
    val f = collection.updateOne(filter, update).toFuture()
    f onFailure errorHandler()
    f
  }

  def clearAllSubTasks(_id:ReportID): Future[UpdateResult] ={
    val updates = Updates.set("tasks", Seq.empty[SubTask])
    val f = collection.updateOne(Filters.equal("_id", _id), updates).toFuture()
    f onFailure(errorHandler())
    f
  }

  def delete (_id:ReportID) = {
    val filter = Filters.and(Filters.equal("_id", _id))
    val f = collection.deleteOne(filter).toFuture()
    f onFailure errorHandler
    f
  }
}