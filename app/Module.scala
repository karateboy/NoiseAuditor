import com.google.inject.AbstractModule
import models._
import play.api._
import play.api.libs.concurrent.AkkaGuiceSupport
/**
 * This class is a Guice module that tells Guice how to bind several
 * different types. This Guice module is created when the Play
 * application starts.

 * Play will automatically use any class called `Module` that is in
 * the root package. You can create modules in other locations by
 * adding `play.modules.enabled` settings to the `application.conf`
 * configuration file.
 */
class Module extends AbstractModule with AkkaGuiceSupport {
  Logger.info("Module...")
  override def configure() = {
    bind(classOf[MongoDB])
    bind(classOf[MonitorTypeOp])

    // bindActor[DataCollectManager]("dataCollectManager")

    //bind(classOf[ForwardManager])
    // Use the system clock as the default implementation of Clock
    //bind(classOf[Clock]).toInstance(Clock.systemDefaultZone)
    // Ask Guice to create an instance of ApplicationTimer when the
    // application starts.
    //bind(classOf[ApplicationTimer]).asEagerSingleton()
    // Set AtomicCounter as the implementation for Counter.
    //bind(classOf[Counter]).to(classOf[AtomicCounter])
    //bind(classOf[MonitorTypeDB]).asEagerSingleton()
    //bind(classOf[OmronPlc]).asEagerSingleton()
    /*
    def init(){
    val f = database.listCollectionNames().toFuture()
    val colFuture = f.map { colNames =>
      SysConfig.init(colNames)
      //MonitorType =>
      val mtFuture = MonitorType.init(colNames)
      ModelHelper.waitReadyResult(mtFuture)
      Instrument.init(colNames)
      Record.init(colNames)
      User.init(colNames)
      Calibration.init(colNames)
      MonitorStatus.init(colNames)
      Alarm.init(colNames)
      InstrumentStatus.init(colNames)
      ManualAuditLog.init(colNames)
    }
    //Program need to wait before init complete
    import scala.concurrent.Await
    import scala.concurrent.duration._
    import scala.language.postfixOps

    Await.result(colFuture, 30 seconds)
  }
     */
  }

}
