package models
import models.ModelHelper.errorHandler
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.model._
import play.api.Logger

import java.util.Date
import scala.concurrent.ExecutionContext.Implicits.global
import javax.inject.{Inject, Singleton}
import scala.concurrent.Future
import scala.util.{Failure, Success}


case class AuditLogID(reportID: ReportID, mntNum:Int)
case class LogEntry(mntNum:Int, time:Date, dataType:String, msg:String)
case class AuditLog(_id: AuditLogID, reportTolerance:ReportTolerance, logs:Seq[LogEntry] = Seq.empty[LogEntry])
object AuditLog {
  val DataTypeNoiseSec = "每秒噪音監測資料"
  val DataTypeNoiseEvent = "噪音事件監測資料"
  val DataTypeNoiseHour = "每小時噪音監測資料"
  val DataTypeNoiseDay = "每日噪音監測資料"
  val DataTypeNoiseMonth = "每月噪音監測資料"
  val DataTypeNoiseQuarter = "每季噪音監測資料"
  val DataTypeNoiseYear = "一年噪音監測資料"
}

@Singleton
class AuditLogOp @Inject()(mongoDB: MongoDB){
  import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
  import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
  import org.mongodb.scala._
  import org.mongodb.scala.bson.codecs.Macros._
  import org.mongodb.scala.model._

  val ColName = "auditLogs"
  val codecRegistry = fromRegistries(fromProviders(classOf[AuditLog], classOf[AuditLogID], classOf[ReportID], classOf[AirportInfoID], classOf[ReportTolerance], classOf[LogEntry]), DEFAULT_CODEC_REGISTRY)
  val collection: MongoCollection[AuditLog] = mongoDB.database.withCodecRegistry(codecRegistry).getCollection(ColName)

  def init(): Unit = {
    for(colNames <- mongoDB.database.listCollectionNames().toFuture()) {
      if (!colNames.contains(ColName)) {
        val f = mongoDB.database.createCollection(ColName).toFuture()
        f.onFailure(errorHandler)
        f onComplete({
          case Success(_) =>
          case Failure(ex)=>
            Logger.error("failed to init auditLogs collection", ex)
        })
      }
    }
  }

  init

  def upsert(log:AuditLog)={
    val f = collection.replaceOne(Filters.equal("_id", log._id), log,
      ReplaceOptions().upsert(true)).toFuture()
    f onFailure(errorHandler)
    f
  }

  def appendLog(_id:AuditLogID, logs:Seq[LogEntry])={
    // FIXME only log day
    val update = Updates.pushEach("logs", logs:_*)
    val f = collection.updateOne(Filters.equal("_id", _id), update).toFuture()
    f onFailure(errorHandler())
    f
  }

  def get(_id:AuditLogID): Future[AuditLog] ={
    val f = collection.find(Filters.equal("_id", _id)).first().toFuture()
    f onFailure(errorHandler())
    f
  }

}
