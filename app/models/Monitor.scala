package models

import models.ModelHelper._
import org.mongodb.scala.model._
import play.api._
import play.api.libs.json._

import scala.concurrent.ExecutionContext.Implicits.global

case class Monitor(_id: String, desc: String, monitorTypes: Seq[String] = Seq.empty[String])

import javax.inject._
object Monitor{
  val SELF_ID = "me"
  val selfMonitor = Monitor(SELF_ID, "本站")
}
@Singleton
class MonitorOp @Inject()(mongoDB: MongoDB, config: Configuration) {
  implicit val mWrite = Json.writes[Monitor]
  implicit val mRead = Json.reads[Monitor]

  import Monitor._
  import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
  import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
  import org.mongodb.scala.bson.codecs.Macros._

  val colName = "monitors"
  mongoDB.database.createCollection(colName).toFuture()

  val codecRegistry = fromRegistries(fromProviders(classOf[Monitor]), DEFAULT_CODEC_REGISTRY)
  val collection = mongoDB.database.getCollection[Monitor](colName).withCodecRegistry(codecRegistry)
  val hasSelfMonitor = config.getBoolean("selfMonitor").getOrElse(false)

  var map: Map[String, Monitor] = {
    val pairs =
      for (m <- mList) yield {
        m._id -> m
      }
    pairs.toMap
  }

  def init(): Unit = {
    val colNames = waitReadyResult(mongoDB.database.listCollectionNames().toFuture())
    if (!colNames.contains(colName)) {
      val f = mongoDB.database.createCollection(colName).toFuture()
      f.onFailure(errorHandler)
    }

    val ret = waitReadyResult(collection.countDocuments(Filters.equal("_id", SELF_ID)).toFuture())
    if (ret == 0) {
      waitReadyResult(collection.insertOne(selfMonitor).toFuture())
    }
  }

  init
  refresh

  def mvList = mList.map(_._id).filter({
    p =>
      hasSelfMonitor || p != SELF_ID
  })

  def ensureMonitor(_id: String) = {
    if (!map.contains(_id)) {
      newMonitor(Monitor(_id, _id))
    }
  }

  def ensureMonitor(_id: String, monitorTypes: Seq[String]) = {
    if (!map.contains(_id)) {
      newMonitor(Monitor(_id, _id, monitorTypes))
    }
  }

  def newMonitor(m: Monitor) = {
    Logger.debug(s"Create monitor value ${m._id}!")
    map = map + (m._id -> m)

    val f = collection.insertOne(m).toFuture()
    f.onFailure(errorHandler)
    f.onSuccess({
      case _: Seq[t] =>
    })
    waitReadyResult(f)
    m._id
  }

  def format(v: Option[Double]) = {
    if (v.isEmpty)
      "-"
    else
      v.get.toString
  }

  def upsert(m: Monitor) = {
    val f = collection.replaceOne(Filters.equal("_id", m._id), m, ReplaceOptions().upsert(true)).toFuture()
    f.onFailure(errorHandler)
    waitReadyResult(f)
    refresh
  }

  def refresh {
    val pairs =
      for (m <- mList) yield {
        m._id -> m
      }
    map = pairs.toMap
  }

  private def mList: List[Monitor] = {
    val f = collection.find().sort(Sorts.ascending("_id")).toFuture()
    val ret = waitReadyResult(f)
    ret.toList
  }

  def deleteMonitor(_id:String) = {
    val f = collection.deleteOne(Filters.equal("_id", _id)).toFuture()
    f.andThen({
      case _=>
        map = map.filter(p => p._1 != _id)
    })
  }
}