package models

import models.ModelHelper._
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.model.{Filters, Updates}
import play.api._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.implicitConversions


case class User(_id: String, password: String, name: String, isAdmin: Boolean, group: Option[String], monitorTypeOfInterest: Seq[String])

import javax.inject._

@Singleton
class UserOp @Inject()(mongoDB: MongoDB, groupOp: GroupOp) {

  import org.mongodb.scala._

  val ColName = "users"
  val codecRegistry = fromRegistries(fromProviders(classOf[User], classOf[AlarmConfig]), DEFAULT_CODEC_REGISTRY)
  val collection: MongoCollection[User] = mongoDB.database.withCodecRegistry(codecRegistry).getCollection(ColName)

  def init() {
    for (colNames <- mongoDB.database.listCollectionNames().toFuture()) {
      if (!colNames.contains(ColName)) {
        val f = mongoDB.database.createCollection(ColName).toFuture()
        f.onFailure(errorHandler)
      }
    }

    val f = collection.countDocuments().toFuture()
    f.onSuccess({
      case count: Long =>
        if (count == 0) {
          val defaultUser = User("sales@wecc.com.tw", "abc123", "Aragorn", true, Some(Group.PLATFORM_ADMIN),
            Seq.empty[String])
          Logger.info("Create default user:" + defaultUser.toString())
          newUser(defaultUser)
        }
    })
    f.onFailure(errorHandler)
  }

  init

  def newUser(user: User) = {
    val f = collection.insertOne(user).toFuture()
    waitReadyResult(f)
  }

  import org.mongodb.scala.model.Filters._

  def deleteUser(email: String) = {
    val f = collection.deleteOne(equal("_id", email)).toFuture()
    waitReadyResult(f)
  }

  def updateUser(user: User) = {
    if (user.password != "") {
      val f = collection.replaceOne(equal("_id", user._id), user).toFuture()
      waitReadyResult(f)
    } else {
      val updates = {
        if (user.group.isEmpty)
          Updates.combine(
            Updates.set("name", user.name),
            Updates.set("isAdmin", user.isAdmin),
            Updates.set("monitorTypeOfInterest", user.monitorTypeOfInterest)
          )
        else
          Updates.combine(
            Updates.set("name", user.name),
            Updates.set("isAdmin", user.isAdmin),
            Updates.set("group", user.group.get),
            Updates.set("monitorTypeOfInterest", user.monitorTypeOfInterest)
          )
      }

      val f = collection.findOneAndUpdate(equal("_id", user._id), updates).toFuture()
      waitReadyResult(f)
    }

  }

  def getUserByEmail(email: String) = {
    val f = collection.find(equal("_id", email)).first().toFuture()
    f.onFailure {
      errorHandler
    }
    val user = waitReadyResult(f)
    if (user != null)
      Some(user)
    else
      None
  }

  def getAllUsers() = {
    val f = collection.find().toFuture()
    f.onFailure {
      errorHandler
    }
    waitReadyResult(f)
  }

  def getAdminUsers() = {
    val f = collection.find(equal("isAdmin", true)).toFuture()
    f.onFailure {
      errorHandler
    }
    waitReadyResult(f)
  }

}
