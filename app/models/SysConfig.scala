package models

import models.ModelHelper._
import org.mongodb.scala.bson._
import org.mongodb.scala.model._
import play.api.libs.json._

import javax.inject._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.implicitConversions

@Singleton
class SysConfig @Inject()(mongoDB: MongoDB) {
  val ColName = "sysConfig"
  val collection = mongoDB.database.getCollection(ColName)

  val valueKey = "value"
  val MonitorTypeVer = "Version"
  val ReportToleranceKey = "ReportTolerance"

  import ReportTolerance._

  val defaultConfig = Map(
    MonitorTypeVer -> Document(valueKey -> 1),
    ReportToleranceKey -> Document(valueKey -> Json.toJson(ReportTolerance.default).toString())
  )

  def init() {
    for (colNames <- mongoDB.database.listCollectionNames().toFuture()) {
      if (!colNames.contains(ColName)) {
        val f = mongoDB.database.createCollection(ColName).toFuture()
        f.onFailure(errorHandler)
        waitReadyResult(f)
      }
    }
    val values = Seq.empty[String]
    val idSet = values

    //Clean up unused
    val f1 = collection.deleteMany(Filters.not(Filters.in("_id", idSet.toList: _*))).toFuture()
    f1.onFailure(errorHandler)
    val updateModels =
      for ((k, defaultDoc) <- defaultConfig) yield {
        UpdateOneModel(
          Filters.eq("_id", k),
          Updates.setOnInsert(valueKey, defaultDoc(valueKey)), UpdateOptions().upsert(true))
      }

    val f2 = collection.bulkWrite(updateModels.toList, BulkWriteOptions().ordered(false)).toFuture()

    import scala.concurrent._
    val f = Future.sequence(List(f1, f2))
    waitReadyResult(f)
  }

  init

  def upsert(_id: String, doc: Document) = {
    val uo = new ReplaceOptions().upsert(true)
    val f = collection.replaceOne(Filters.equal("_id", _id), doc, uo).toFuture()
    f.onFailure(errorHandler)
    f
  }

  def get(_id: String) = {
    val f = collection.find(Filters.eq("_id", _id.toString())).headOption()
    f.onFailure(errorHandler)
    for (ret <- f) yield {
      val doc = ret.getOrElse(defaultConfig(_id))
      doc("value")
    }
  }

  def set(_id: String, v: BsonValue) = upsert(_id, Document(valueKey -> v))
}