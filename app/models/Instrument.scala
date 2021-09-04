package models

import models.Protocol.ProtocolParam
import play.api.libs.json._

import javax.inject._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Success
case class InstrumentInfo(_id: String, instType: String, state: String,
                          protocol: String, protocolParam: String, monitorTypes: String,
                          calibrationTime: Option[String], inst:Instrument)

case class InstrumentStatusType(key:String, addr:Int, desc:String, unit:String)

case class Instrument(_id: String, instType: String,
                      protocol: ProtocolParam, param: String, active: Boolean, 
                      state: String,
                      statusType:Option[List[InstrumentStatusType]]) {

  /*
  def getMonitorTypes: List[String] = {
    val instTypeCase = instrumentTypeOp.map(instType)
    instTypeCase.driver.getMonitorTypes(param)
  }

  def getCalibrationTime = {
    val instTypeCase = instrumentTypeOp.map(instType)
    instTypeCase.driver.getCalibrationTime(param)
  }

  def getStateStr = {
    if (active){
      monitorStatusOp.map(state).desp
    }else
      "停用"
  }
  
  def getInfoClass = {
    val mtStr = getMonitorTypes.map { monitorTypeOp.map(_).desp }.mkString(",")
    val protocolParam =
      protocol.protocol match {
        case Protocol.tcp =>
          protocol.host.get
        case Protocol.serial =>
          s"COM${protocol.comPort.get}"
      }
    val calibrationTime = getCalibrationTime.map { t => t.toString("HH:mm") }

    val state = getStateStr

    InstrumentInfo(_id, instrumentTypeOp.map(instType).desp, state, Protocol.map(protocol.protocol), protocolParam, mtStr, calibrationTime)
  }
  */
  def replaceParam(newParam: String) = {
    Instrument(_id, instType, protocol, newParam, active, state, statusType)
  }
}

import models.ModelHelper._
import org.mongodb.scala._

@Singleton
class InstrumentOp @Inject() (mongoDB: MongoDB) {
  implicit val ipRead = Json.reads[InstrumentStatusType]
  implicit val reader = Json.reads[Instrument]
  implicit val ipWrite = Json.writes[InstrumentStatusType]
  implicit val writer = Json.writes[Instrument]
  implicit val infoWrites = Json.writes[InstrumentInfo]

  val collectionName = "instruments"
  val collection: MongoCollection[Document] = mongoDB.database.getCollection(collectionName)
  def toDocument(inst: Instrument) = {
    val json = Json.toJson(inst)
    val doc = Document(json.toString())
    val param = doc.get("param").get.asString().getValue

    doc
  }

  def toInstrument(doc: Document) = {
    //val param = doc.get("param").get.asDocument().toJson()
    //val doc1 = doc ++ Document("param" -> param)

    val ret = Json.parse(doc.toJson()).validate[Instrument]
    ret.fold(error => {
      throw new Exception(JsError.toJson(error).toString)
    },
      v => { v })
  }

  def init() {
    for(colNames <- mongoDB.database.listCollectionNames().toFuture()) {
      if (!colNames.contains(collectionName)) {
        val f = mongoDB.database.createCollection(collectionName).toFuture()
        f.onFailure(errorHandler)
      }
    }
  }
  init

  import org.mongodb.scala.model.Filters._
  def upsertInstrument(inst: Instrument) = {    
    import org.mongodb.scala.model.ReplaceOptions
    val f = collection.replaceOne(equal("_id", inst._id), toDocument(inst), ReplaceOptions().upsert(true)).toFuture()
    waitReadyResult(f)
    true
  }

  def getInstrumentList() = {
    val f = collection.find().toFuture()
    waitReadyResult(f).map { toInstrument }
  }

  def getInstrumentMap() = {
    val f = collection.find().toFuture()
    for(docs<-f) yield {
      val instruments = docs map {toInstrument}
      instruments.map(inst=>inst._id->inst).toMap
    }
  }
  
  def getInstrument(id: String) = {
    val f = collection.find(equal("_id", id)).toFuture()
    waitReadyResult(f).map { toInstrument }
  }

  def getInstrumentFuture(id: String):Future[Instrument] = {
    val f = collection.find(equal("_id", id)).first().toFuture()
    for(doc <- f) yield
      toInstrument(doc)
  }

  def getAllInstrumentFuture = {
    for(docs <- collection.find().toFuture())
      yield
        docs.map { toInstrument }
  }

  def delete(id: String) = {
    val f = collection.deleteOne(equal("_id", id)).toFuture()
    waitReadyResult(f)
    true
  }

  def activate(id: String) = {
    import org.mongodb.scala.model.Updates._
    val f = collection.updateOne(equal("_id", id), set("active", true)).toFuture()
    f.onFailure({
      case ex:Exception=>
        ModelHelper.logException(ex)
    })
    f
  }

  def deactivate(id: String) = {
    import org.mongodb.scala.model.Updates._
    val f = collection.updateOne(equal("_id", id), set("active", false)).toFuture()
    f.onFailure({
      case ex:Exception=>
        ModelHelper.logException(ex)
    })
    f
  }

  def setState(id:String, state:String) = {
    import org.mongodb.scala.model.Updates._    
    val f = collection.updateOne(equal("_id", id), set("state", state)).toFuture()
    f.onFailure({
      case ex:Exception=>
        ModelHelper.logException(ex)
    })
    f
  }
  
  def updateStatusType(id:String, status:List[InstrumentStatusType]) = {
    import org.mongodb.scala.bson.BsonArray
    import org.mongodb.scala.model.Updates._
    val bArray = new BsonArray
    
    val statusDoc = status.map{ s => bArray.add(Document(Json.toJson(s).toString).toBsonDocument)}
    
    val f = collection.updateOne(equal("_id", id), set("statusType", bArray)).toFuture()
    f.onFailure({
      case ex:Exception=>
        ModelHelper.logException(ex)
    })
    f
  }
  
  def getStatusTypeMap(id:String) = {
    val instList = getInstrument(id)
    if(instList.length != 1)
      throw new Exception("no such Instrument")
    
    val inst = instList(0)
    
    val statusTypeOpt = inst.statusType
    if(statusTypeOpt.isEmpty)
      Map.empty[String, String]
    else{
      val statusType = statusTypeOpt.get 
      val kv =
        for(kv <- statusType)
          yield
          kv.key -> kv.desc
      
      Map(kv:_*)
    }
  }
}