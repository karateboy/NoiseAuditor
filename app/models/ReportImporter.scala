package models
import akka.actor._
import play.api._

import java.io.File
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, blocking}
import scala.util.{Failure, Success}

object ReportImporter {
  var n = 0
  private var actorRefMap = Map.empty[String, ActorRef]

  def start(dataFile: File)(implicit actorSystem: ActorSystem) = {
    val name = getName
    val actorRef = actorSystem.actorOf(ReportImporter.props(dataFile = dataFile), name)
    actorRefMap = actorRefMap + (name -> actorRef)
    name
  }

  def getName = {
    n = n + 1
    s"dataImporter${n}"
  }

  def props(dataFile: File) =
    Props(classOf[ReportImporter], dataFile)

  def finish(actorName: String) = {
    actorRefMap = actorRefMap.filter(p => {
      p._1 != actorName
    })
  }

  def isFinished(actorName: String) = {
    !actorRefMap.contains(actorName)
  }

  sealed trait FileType

  case object Import

  case object Complete
}

class ReportImporter(dataFile: File) extends Actor {
  import ReportImporter._

  self ! Import

  override def receive: Receive = {
    case Import =>
      Future {
        blocking {
          try {
            dataFile.delete()
            Thread.sleep(1000)
            self ! Complete
          } catch {
            case ex: Exception =>
              Logger.error("failed to import", ex)
          }
        }
      }
    case Complete =>
      finish(context.self.path.name)
      self ! PoisonPill
  }
}
