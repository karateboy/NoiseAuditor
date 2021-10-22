package models

import models.ModelHelper.errorHandler
import org.mongodb.scala.result.UpdateResult
import play.api.Logger

import javax.inject.{Inject, Singleton}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
case class DataFormatError(fileName:String, terminal: String, time:String, dataType:String,
                           fieldName:String, errorInfo:String, value:String)
case class ImportLogID(reportID:ReportID, folderType:String)
case class ImportErrorLog(_id:ImportLogID, logs: Seq[DataFormatError])
@Singleton
class ImportErrorLogOp @Inject()(mongoDB: MongoDB) {
  import org.mongodb.scala._
  import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
  import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
  import org.mongodb.scala.bson.codecs.Macros._
  import org.mongodb.scala.model._

  val ColName = "importErrorLogs"
  val codecRegistry = fromRegistries(fromProviders(classOf[ImportErrorLog], classOf[ImportLogID],
    classOf[ReportID], classOf[AirportInfoID], classOf[DataFormatError]), DEFAULT_CODEC_REGISTRY)
  val collection: MongoCollection[ImportErrorLog] = mongoDB.database.withCodecRegistry(codecRegistry).getCollection(ColName)

  def init(): Unit = {
    for(colNames <- mongoDB.database.listCollectionNames().toFuture()) {
      if (!colNames.contains(ColName)) {
        val f = mongoDB.database.createCollection(ColName).toFuture()
        f.onFailure(errorHandler)
        f onComplete({
          case Success(_) =>
          case Failure(ex)=>
            Logger.error("failed to init imortErrorLogs collection", ex)
        })
      }
    }
  }

  init

  def appendDataFormatErrors(_id:ImportLogID, dfeList: Seq[DataFormatError]) = {
    val update = Updates.pushEach("logs", dfeList:_*)
    val opt = UpdateOptions().upsert(true)
    val f = collection.updateOne(Filters.equal("_id", _id), update, opt).toFuture()
    f onFailure(errorHandler())
    f
  }

  def upsert(log:ImportErrorLog) = {
    val f = collection.replaceOne(Filters.equal("_id", log._id), log,
      ReplaceOptions().upsert(true)).toFuture()
    f onFailure(errorHandler)
    f
  }

  def delete(_id:ReportID) = {
    val filter = Filters.equal("_id.reportID", _id)
    val f = collection.deleteMany(filter).toFuture()
    f onFailure(errorHandler)
    f
  }
}
