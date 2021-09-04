package models

import models.ModelHelper.errorHandler
import scala.concurrent.ExecutionContext.Implicits.global
import javax.inject.{Inject, Singleton}
import scala.concurrent.Future

case class Airport(_id:Int, name:String, county:String)
object Airport {

}

@Singleton
class AirportOp @Inject()(mongoDB: MongoDB) {
  import org.mongodb.scala._
  import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
  import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
  import org.mongodb.scala.bson.codecs.Macros._
  import org.mongodb.scala.model._

  val ColName = "airports"
  val codecRegistry = fromRegistries(fromProviders(classOf[Airport]), DEFAULT_CODEC_REGISTRY)
  val collection: MongoCollection[Airport] = mongoDB.database.withCodecRegistry(codecRegistry).getCollection(ColName)

  def getAirportList(): Future[Seq[Airport]] ={
    val f = collection.find(Filters.exists("_id")).toFuture()
    f onFailure(errorHandler)
    f
  }

  def upsertReportInfo(airport: Airport) = {
    val f = collection.replaceOne(Filters.equal("_id", airport._id), airport,
      ReplaceOptions().upsert(true)).toFuture()
    f onFailure(errorHandler)
    f
  }
}