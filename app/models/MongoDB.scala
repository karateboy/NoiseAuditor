package models
import play.api._
import javax.inject._
import scala.concurrent.ExecutionContext.Implicits.global

@Singleton
class MongoDB @Inject() (config: Configuration){
  import org.mongodb.scala._

  val url = config.getString("my.mongodb.url")
  val dbName = config.getString("my.mongodb.db")
  
  val mongoClient: MongoClient = MongoClient(url.get)
  val database: MongoDatabase = mongoClient.getDatabase(dbName.get);

  
  def cleanup={
    mongoClient.close()
  }
}