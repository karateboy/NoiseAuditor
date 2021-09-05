package models

import models.ModelHelper.errorHandler
import play.api.Logger

import javax.inject.{Inject, Singleton}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

case class Airport(_id: Int, name: String)
object Airport {

}

@Singleton
class AirportOp @Inject()(mongoDB: MongoDB) {

  import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
  import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
  import org.mongodb.scala._
  import org.mongodb.scala.bson.codecs.Macros._
  import org.mongodb.scala.model._

  val ColName = "airports"
  val codecRegistry = fromRegistries(fromProviders(classOf[Airport]), DEFAULT_CODEC_REGISTRY)
  val collection: MongoCollection[Airport] = mongoDB.database.withCodecRegistry(codecRegistry).getCollection(ColName)

  def getAirportList(): Future[Seq[Airport]] = {
    val f = collection.find(Filters.exists("_id")).toFuture()
    f onFailure (errorHandler)
    f
  }

  val defaultAirports = Seq(
    Airport(1, "台北松山機場"),
    Airport(2, "台灣桃園國際機場"),
    Airport(3, "空軍第二戰術戰鬥機聯隊（新竹機場）"),
    Airport(4, "空軍第一戰術戰鬥機聯隊(台南機場)"),
    Airport(5, "空軍第三戰術戰鬥機聯隊（台中清泉崗機場）"),
    Airport(6, "陸軍航空六O二旅(台中新社基地)"),
    Airport(7, "澎湖馬公機場"),
    Airport(8, "空軍第四戰術戰鬥機聯隊(嘉義水上機場)"),
    Airport(9, "陸軍飛行訓練指揮部(台南歸仁基地)"),
    Airport(10, "陸軍航空六O一旅(桃園龍潭基地)"),
    Airport(11, "高雄小港機場"),
    Airport(12, "空軍官校 (高雄岡山基地)"),
    Airport(13, "金門尚義機場"),
    Airport(14, "台東豐年機場"),
    Airport(15, "空軍第七飛行訓練聯隊(台東志航基地)"),
    Airport(16, "空軍第五戰術混合聯隊(花蓮機場)"),
    Airport(17, "空軍第六混合聯隊(屏東機場)")
  )
  def init(): Unit = {
    for(colNames <- mongoDB.database.listCollectionNames().toFuture()) {
      if (!colNames.contains(ColName)) {
        val f = mongoDB.database.createCollection(ColName).toFuture()
        f.onFailure(errorHandler)
        f onComplete({
          case Success(_) =>
            collection.insertMany(defaultAirports).toFuture()
          case Failure(ex)=>
            Logger.error("failed to init airport collection", ex)
        })
      }
    }
  }

  init

  def upsertAirport(airport: Airport) = {
    val f = collection.replaceOne(Filters.equal("_id", airport._id), airport,
      ReplaceOptions().upsert(true)).toFuture()
    f onFailure (errorHandler)
    f
  }

  def getList()={
    val f = collection.find(Filters.exists("_id")).toFuture()
    f onFailure(errorHandler)
    f
  }
}