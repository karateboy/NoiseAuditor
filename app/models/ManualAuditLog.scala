package models
import play.api._
import com.github.nscala_time.time.Imports._
import models.ModelHelper._
import models._
import scala.concurrent.ExecutionContext.Implicits.global
import org.mongodb.scala._

case class ManualAuditLog(dataTime: DateTime, mt: String, modifiedTime: DateTime,
                          operator: String, changedStatus: String, reason: String)

case class ManualAuditLog2(dataTime: Long, mt: String, modifiedTime: Long,
                          operator: String, changedStatus: String, reason: String)

import javax.inject._

@Singleton
class ManualAuditLogOp @Inject()(mongoDB: MongoDB, monitorTypeOp: MonitorTypeOp) {
  import play.api.libs.json._
  import play.api.libs.functional.syntax._

  implicit val writer = Json.writes[ManualAuditLog]
  val collectionName = "auditLogs"
  val collection = mongoDB.database.getCollection(collectionName)
  def toDocument(al: ManualAuditLog) = {
    import org.mongodb.scala.bson._
    Document("dataTime" -> (al.dataTime: BsonDateTime), "mt" -> monitorTypeOp.BFName(al.mt),
      "modifiedTime" -> (al.modifiedTime: BsonDateTime), "operator" -> al.operator, "changedStatus" -> al.changedStatus, "reason" -> al.reason)
  }

  def toAuditLog(doc: Document) = {
    val dataTime = new DateTime(doc.get("dataTime").get.asDateTime().getValue)
    val mt = (doc.get("mt").get.asString().getValue)
    val modifiedTime = new DateTime(doc.get("modifiedTime").get.asDateTime().getValue)
    val operator = doc.get("operator").get.asString().getValue
    val changedStatus = doc.get("changedStatus").get.asString().getValue
    val reason = doc.get("reason").get.asString().getValue

    ManualAuditLog(dataTime = dataTime, mt = mt, modifiedTime = modifiedTime, operator = operator, changedStatus = changedStatus, reason = reason)
  }

  def toAuditLog2(doc: Document) = {
    val dataTime = new DateTime(doc.get("dataTime").get.asDateTime().getValue)
    val mt = (doc.get("mt").get.asString().getValue)
    val modifiedTime = new DateTime(doc.get("modifiedTime").get.asDateTime().getValue)
    val operator = doc.get("operator").get.asString().getValue
    val changedStatus = doc.get("changedStatus").get.asString().getValue
    val reason = doc.get("reason").get.asString().getValue

    ManualAuditLog2(dataTime = dataTime.getMillis, mt = mt, modifiedTime = modifiedTime.getMillis, operator = operator, changedStatus = changedStatus, reason = reason)
  }

  def init() {
    import org.mongodb.scala.model.Indexes._
    for(colNames <- mongoDB.database.listCollectionNames().toFuture()) {
      if (!colNames.contains(collectionName)) {
        val f = mongoDB.database.createCollection(collectionName).toFuture()
        f.onFailure(errorHandler)
        f.onSuccess({
          case _ =>
            collection.createIndex(ascending("dataTime", "mt"))
        })
      }
    }
  }
  init

  import org.mongodb.scala.model.Filters._
  def upsertLog(log: ManualAuditLog) = {
    import org.mongodb.scala.model.ReplaceOptions
    import org.mongodb.scala.bson.BsonDateTime
    val f = collection.replaceOne(and(equal("dataTime", log.dataTime:BsonDateTime), equal("mt", monitorTypeOp.BFName(log.mt))),
      toDocument(log), ReplaceOptions().upsert(true)).toFuture()
      
    f.onFailure(errorHandler)
    f
  }

  def queryLog(startTime: DateTime, endTime: DateTime) = {
    import org.mongodb.scala.model.Filters._
    import org.mongodb.scala.model.Projections._
    import org.mongodb.scala.model.Sorts._
    import org.mongodb.scala.bson.BsonDateTime

    val future = collection.find(and(gte("dataTime", startTime:BsonDateTime), lt("dataTime", endTime:BsonDateTime))).sort(ascending("dataTime")).toFuture()
    for (f <- future) yield {
      f.map { toAuditLog }
    }
  }

  def queryLog2(startTime: DateTime, endTime: DateTime) = {
    import org.mongodb.scala.model.Filters._
    import org.mongodb.scala.model.Projections._
    import org.mongodb.scala.model.Sorts._
    import org.mongodb.scala.bson.BsonDateTime

    val future = collection.find(and(gte("dataTime", startTime:BsonDateTime), lt("dataTime", endTime:BsonDateTime))).sort(ascending("dataTime")).toFuture()
    for (f <- future) yield {
      f.map { toAuditLog2 }
    }
  }
}