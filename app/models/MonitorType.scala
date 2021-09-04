package models

import com.github.nscala_time.time.Imports._
import models.ModelHelper._
import org.mongodb.scala.model._
import play.api._
import play.api.libs.json._

case class ThresholdConfig(elapseTime:Int)
case class MonitorType(_id: String, desp: String, unit: String,
                       prec: Int, order: Int,
                       signalType: Boolean = false,
                       std_law: Option[Double] = None,
                       std_internal: Option[Double] = None,
                       zd_internal: Option[Double] = None, zd_law: Option[Double] = None,
                       span: Option[Double] = None, span_dev_internal: Option[Double] = None, span_dev_law: Option[Double] = None,
                       var measuringBy: Option[List[String]] = None,
                       thresholdConfig: Option[ThresholdConfig] = None) {
  def defaultUpdate = {
    Updates.combine(
      Updates.setOnInsert("_id", _id),
      Updates.setOnInsert("desp", desp),
      Updates.setOnInsert("unit", unit),
      Updates.setOnInsert("prec", prec),
      Updates.setOnInsert("order", order),
      Updates.setOnInsert("signalType", signalType))
  }

  def addMeasuring(instrumentId: String, append: Boolean) = {
    val newMeasuringBy =
      if (measuringBy.isEmpty)
        List(instrumentId)
      else {
        val currentMeasuring = measuringBy.get
        if (currentMeasuring.contains(instrumentId))
          currentMeasuring
        else {
          if (append)
            measuringBy.get ++ List(instrumentId)
          else
            instrumentId :: measuringBy.get
        }
      }

    MonitorType(_id, desp, unit,
      prec, order, signalType, std_law, std_internal,
      zd_internal, zd_law,
      span, span_dev_internal, span_dev_law,
      Some(newMeasuringBy))
  }

  def stopMeasuring(instrumentId: String) = {
    val newMeasuringBy =
      if (measuringBy.isEmpty)
        None
      else
        Some(measuringBy.get.filter { id => id != instrumentId })

    MonitorType(_id, desp, unit,
      prec, order, signalType, std_law, std_internal,
      zd_internal, zd_law,
      span, span_dev_internal, span_dev_law,
      newMeasuringBy)
  }
}

//MeasuredBy => History...
//MeasuringBy => Current...

import javax.inject._

object MonitorType {
  val SO2 = "SO2"
  val NOx = "NOx"
  val NO2 = "NO2"
  val NO = "NO"
  val NOX = "NOX"
  val CO = "CO"
  val CO2 = "CO2"
  val CH4 = "CH4"
  val PM10 = "PM10"
  val PM25 = "PM25"
  val O3 = "O3"
  val THC = "THC"

  val LAT = "LAT"
  val LNG = "LNG"
  val WIN_SPEED = "WD_SPEED"
  val WIN_DIRECTION = "WD_DIR"
  val RAIN = "RAIN"
  val TEMP= "TEMP"
  val TS = "TS"
  val PRESS = "PRESS"
  val DOOR = "DOOR"
  val SMOKE = "SMOKE"
  val FLOW = "FLOW"
  val HUMID ="HUMID"
  val SOLAR = "SOLAR"
  val CH2O = "CH2O"
  val TVOC = "TVOC"
  val NOISE = "NOISE"
  val H2S = "H2S"
  val H2 = "H2"
  val NH3 = "NH3"
  val NOY_NO = "NOY-NO"
  val NOY = "NOY"
  var rangeOrder = 0
  var signalOrder = 1000

}
@Singleton
class MonitorTypeOp @Inject()(mongoDB: MongoDB, alarmOp: AlarmOp) {

  import org.mongodb.scala.bson._
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent._
  import MonitorType._

  implicit val configWrite = Json.writes[ThresholdConfig]
  implicit val configRead = Json.reads[ThresholdConfig]
  implicit val mtWrite = Json.writes[MonitorType]
  implicit val mtRead = Json.reads[MonitorType]

  implicit object TransformMonitorType extends BsonTransformer[MonitorType] {
    def apply(mt: MonitorType): BsonString = new BsonString(mt.toString)
  }

  import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
  import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
  import org.mongodb.scala.bson.codecs.Macros._

  val codecRegistry = fromRegistries(fromProviders(classOf[MonitorType], classOf[ThresholdConfig]), DEFAULT_CODEC_REGISTRY)
  val colName = "monitorTypes"
  val collection = mongoDB.database.getCollection[MonitorType](colName).withCodecRegistry(codecRegistry)
  val MonitorTypeVer = 2
  val defaultMonitorTypes = List(
    rangeType(SO2, "二氧化硫", "ppb", 2),
    rangeType(NOx, "氮氧化物", "ppb", 2),
    rangeType(NO2, "二氧化氮", "ppb", 2),
    rangeType(NO, "一氧化氮", "ppb", 2),
    rangeType(NOY, "NOY", "ppb", 2),
    rangeType(NOY_NO, "NOY-NO", "ppb", 2),
    rangeType(CO, "一氧化碳", "ppm", 2),
    rangeType(CO2, "二氧化碳", "ppm", 2),
    rangeType(O3, "臭氧", "ppb", 2),
    rangeType(THC, "總碳氫化合物", "ppm", 2),
    rangeType(TS, "總硫", "ppb", 2),
    rangeType(CH4, "甲烷", "ppm", 2),
    rangeType("NMHC", "非甲烷碳氫化合物", "ppm", 2),
    rangeType(NH3, "氨", "ppb", 2),
    rangeType("TSP", "TSP", "μg/m3", 2),
    rangeType(PM10, "PM10懸浮微粒", "μg/m3", 2),
    rangeType(PM25, "PM2.5細懸浮微粒", "μg/m3", 2),
    rangeType(WIN_SPEED, "風速", "m/sec", 2),
    rangeType(WIN_DIRECTION, "風向", "degrees", 2),
    rangeType(TEMP, "溫度", "℃", 2),
    rangeType(HUMID, "濕度", "%", 2),
    rangeType(PRESS, "氣壓", "hPa", 2),
    rangeType(RAIN, "雨量", "mm/h", 2),
    rangeType(LAT, "緯度", "度", 4),
    rangeType(LNG, "經度", "度", 4),
    rangeType("RT", "室內溫度", "℃", 1),
    rangeType("O2", "氧氣 ", "%", 1),
    rangeType(SOLAR, "日照", "W/m2", 2),
    rangeType(CH2O, "CH2O", "ppb", 2),
    rangeType(TVOC, "TVOC", "ppb", 2),
    rangeType(NOISE, "NOISE", "dB", 2),
    rangeType(H2S, "H2S", "ppb", 2),
    rangeType(H2, "H2", "ppb", 2),
    /////////////////////////////////////////////////////
    signalType(DOOR, "門禁"),
    signalType(SMOKE, "煙霧"),
    signalType(FLOW, "採樣流量"),
    signalType("SPRAY", "灑水"))

  var (mtvList, signalMtvList, map) = refreshMtv

  def signalType(_id: String, desp: String) = {
    signalOrder += 1
    MonitorType(_id, desp, "N/A", 0, signalOrder, true)
  }

  private var signalValueMap = Map.empty[String, (DateTime, Boolean)]

  def updateSignalValueMap(mt:String, v:Boolean) = {
    signalValueMap = signalValueMap + (mt -> (DateTime.now, v))
  }

  def getSignalValueMap() ={
    val now = DateTime.now()
    signalValueMap = signalValueMap.filter(p => p._2._1.after(now - 6.seconds))
    signalValueMap map {p => p._1-> p._2._2}
  }

  def logDiMonitorType(mt: String, v: Boolean) = {
    if (!signalMtvList.contains(mt))
      Logger.warn(s"${mt} is not DI monitor type!")

    val mtCase = map(mt)
    if (v) {
      alarmOp.log(alarmOp.Src(), alarmOp.Level.WARN, s"${mtCase.desp}=>觸發", 1)
    }else{
      alarmOp.log(alarmOp.Src(), alarmOp.Level.INFO, s"${mtCase.desp}=>解除", 1)
    }

  }

  def init(): Future[Any] = {
    def updateMt = {
      val updateModels =
        for (mt <- defaultMonitorTypes) yield {
          val filter = Filters.eq("_id", mt._id)
          UpdateOneModel(
            Filters.eq("_id", mt._id),
            mt.defaultUpdate, UpdateOptions().upsert(true))
        }

      val f = collection.bulkWrite(updateModels.toList, BulkWriteOptions().ordered(false)).toFuture()

      f.onFailure(errorHandler)
      f.onComplete { x =>
        refreshMtv
      }
      f
    }

    for(colNames <- mongoDB.database.listCollectionNames().toFuture()) {
      if (!colNames.contains(colName)) { // New
        val f = mongoDB.database.createCollection(colName).toFuture()
        f.onFailure(errorHandler)
        waitReadyResult(f)
      }
    }

    updateMt
  }
  init

  def BFName(mt: String) = {
    val mtCase = map(mt)
    mtCase._id.replace(".", "_")
  }

  def exist(mt: MonitorType) = map.contains(mt._id)

  def ensureMonitorType(id:String) = {
    if(!map.contains(id)){
      val mt = rangeType(id, id, "??", 2)
      newMonitorType(mt)
    }
  }
  def getRawMonitorType(mt: String) =
    (rawMonitorTypeID(mt.toString()))

  def ensureRawMonitorType(mt: String, unit: String) {
    val mtCase = map(mt)

    if (!map.contains(s"${mtCase._id}_raw")) {
      val rawMonitorType = rangeType(
        rawMonitorTypeID(mtCase._id),
        s"${mtCase.desp}(原始值)", unit, 3)
      newMonitorType(rawMonitorType)
    }
  }

  def rangeType(_id: String, desp: String, unit: String, prec: Int) = {
    rangeOrder += 1
    MonitorType(_id, desp, unit, prec, rangeOrder)
  }

  def rawMonitorTypeID(_id: String) = s"${_id}_raw"

  def newMonitorType(mt: MonitorType) = {
    val f = collection.insertOne(mt).toFuture()
    f.onSuccess({
      case x =>
        refreshMtv
    })
  }

  def refreshMtv: (List[String], List[String], Map[String, MonitorType]) = {
    val list = mtList.sortBy {
      _.order
    }
    val mtPair =
      for (mt <- list) yield {
        try {
          val mtv = (mt._id)
          (mtv -> mt)
        } catch {
          case _: NoSuchElementException =>
            (mt._id -> mt)
        }
      }

    val rangeList = list.filter { mt => mt.signalType == false }
    val rangeMtvList = rangeList.map(mt => (mt._id))
    val signalList = list.filter { mt => mt.signalType }
    val signalMvList = signalList.map(mt => (mt._id))
    mtvList = rangeMtvList
    signalMtvList = signalMvList
    map = mtPair.toMap
    (rangeMtvList, signalMvList, mtPair.toMap)
  }

  private def mtList: List[MonitorType] = {
    val f = mongoDB.database.getCollection(colName).find().toFuture()
    val r = waitReadyResult(f)
    r.map {
      toMonitorType
    }.toList
  }

  def toMonitorType(d: Document) = {
    val ret = Json.parse(d.toJson()).validate[MonitorType]

    ret.fold(
      error => {
        Logger.error(JsError.toJson(error).toString())
        throw new Exception(JsError.toJson(error).toString)
      },
      mt =>
        mt)
  }

  def allMtvList = mtvList ++ signalMtvList

  def diMtvList = List(RAIN) ++ signalMtvList

  def activeMtvList = mtvList.filter { mt => map(mt).measuringBy.isDefined }

  def addMeasuring(mt: String, instrumentId: String, append: Boolean) = {
    val newMt = map(mt).addMeasuring(instrumentId, append)
    map = map + (mt -> newMt)
    upsertMonitorTypeFuture(newMt)
  }

  def stopMeasuring(instrumentId: String) = {
    for {
      mt <- realtimeMtvList
      instrumentList = map(mt).measuringBy.get if instrumentList.contains(instrumentId)
    } {
      val newMt = map(mt).stopMeasuring(instrumentId)
      map = map + (mt -> newMt)
      upsertMonitorTypeFuture(newMt)
    }
  }

  def realtimeMtvList = mtvList.filter { mt =>
    val measuringBy = map(mt).measuringBy
    measuringBy.isDefined && (!measuringBy.get.isEmpty)
  }

  import org.mongodb.scala.model.Filters._

  def upsertMonitorTypeFuture(mt: MonitorType) = {
    import org.mongodb.scala.model.ReplaceOptions

    val f = collection.replaceOne(equal("_id", mt._id), mt, ReplaceOptions().upsert(true)).toFuture()
    f.onFailure(errorHandler)
    f
  }

  def upsertMonitorType(mt: MonitorType) = {
    import org.mongodb.scala.model.ReplaceOptions

    val f = collection.replaceOne(equal("_id", mt._id), mt, ReplaceOptions().upsert(true)).toFuture()
    waitReadyResult(f)
    map = map + (mt._id -> mt)
    true
  }

  //  def insertNewMonitorTypeFuture(mt: MonitorType) = {
  //    import org.mongodb.scala.model.UpdateOptions
  //    import org.mongodb.scala.bson.BsonString
  //    import org.mongodb.scala.model.Updates
  //    //Updates.setOnInsert(fieldName, value)
  //
  //    val f = collection.updateOne(equal("_id", mt._id), update, UpdateOptions().upsert(true)).toFuture()
  //    f.onFailure(errorHandler)
  //    f
  //  }

  def updateMonitorType(mt: String, colname: String, newValue: String) = {
    import org.mongodb.scala._
    import org.mongodb.scala.model.Filters._
    import org.mongodb.scala.model.FindOneAndUpdateOptions
    import org.mongodb.scala.model.Updates._
    import java.lang._
    val idFilter = equal("_id", map(mt)._id)
    val opt = FindOneAndUpdateOptions().returnDocument(com.mongodb.client.model.ReturnDocument.AFTER)
    val f =
      if (colname == "desp" || colname == "unit" || colname == "measuringBy" || colname == "measuredBy") {
        if (newValue == "-")
          collection.findOneAndUpdate(idFilter, set(colname, null), opt).toFuture()
        else
          collection.findOneAndUpdate(idFilter, set(colname, newValue), opt).toFuture()
      } else if (colname == "prec" || colname == "order") {
        val v = Integer.parseInt(newValue)
        collection.findOneAndUpdate(idFilter, set(colname, v), opt).toFuture()
      } else {
        if (newValue == "-")
          collection.findOneAndUpdate(idFilter, set(colname, null), opt).toFuture()
        else {
          collection.findOneAndUpdate(idFilter, set(colname, Double.parseDouble(newValue)), opt).toFuture()
        }
      }

    val mtCase = waitReadyResult(f)
    Logger.debug(mtCase.toString)
    map = map + (mt -> mtCase)
  }

  def format(mt: String, v: Option[Double]) = {
    if (v.isEmpty)
      "-"
    else {
      val prec = map(mt).prec
      s"%.${prec}f".format(v.get)
    }
  }

  def getOverStd(mt: String, r: Option[Record]) = {
    if (r.isEmpty)
      false
    else {
      val (overInternal, overLaw) = overStd(mt, r.get.value)
      overInternal || overLaw
    }
  }

  def formatRecord(mt: String, r: Option[Record]) = {
    if (r.isEmpty)
      "-"
    else {
      val (overInternal, overLaw) = overStd(mt, r.get.value)
      val prec = map(mt).prec
      val value = s"%.${prec}f".format(r.get.value)
      if (overInternal || overLaw)
        s"$value"
      else
        s"$value"
    }
  }

  def overStd(mt: String, v: Double) = {
    val mtCase = map(mt)
    val overInternal =
      if (mtCase.std_internal.isDefined) {
        if (v > mtCase.std_internal.get)
          true
        else
          false
      } else
        false
    val overLaw =
      if (mtCase.std_law.isDefined) {
        if (v > mtCase.std_law.get)
          true
        else
          false
      } else
        false
    (overInternal, overLaw)
  }

  def getCssClassStr(record: MtRecord) = {
    val (overInternal, overLaw) = overStd(record.mtName, record.value)
    MonitorStatus.getCssClassStr(record.status, overInternal, overLaw)
  }

  def getCssClassStr(mt: String, r: Option[Record]) = {
    if (r.isEmpty)
      Seq.empty[String]
    else {
      val v = r.get.value
      val (overInternal, overLaw) = overStd(mt, v)
      MonitorStatus.getCssClassStr(r.get.status, overInternal, overLaw)
    }
  }

  def displayMeasuringBy(mt: String) = {
    val mtCase = map(mt)
    if (mtCase.measuringBy.isDefined) {
      val instrumentList = mtCase.measuringBy.get
      if (instrumentList.isEmpty)
        "外部儀器"
      else
        instrumentList.mkString(",")
    } else
      "-"
  }
}