package com.advancedtelematic.director.daemon

import akka.http.scaladsl.util.FastFuture
import cats.implicits.catsSyntaxOptionId
import com.advancedtelematic.director.data.DataType.{ScheduledUpdate, ScheduledUpdateId, StatusInfo}
import com.advancedtelematic.director.db.{ScheduledUpdatesRepositorySupport, UpdateSchedulerDBIO}
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.slf4j.LoggerFactory
import slick.jdbc.MySQLProfile.api.*

import java.time.Instant
import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.concurrent.duration.*

class UpdateSchedulerDaemon()(implicit db: Database, ec: ExecutionContext) {

  private val dbio = new UpdateSchedulerDBIO()

  private lazy val log = LoggerFactory.getLogger(this.getClass)

  private val CHECK_INTERVAL = 1.second

  private val BACKOFF_INTERVAL = 3.seconds

  def start(): Future[Unit] = {
    log.info(
      s"starting update scheduler daemon. CHECK_INTERVAL=$CHECK_INTERVAL BACKOFF_INTERVAL=$BACKOFF_INTERVAL"
    )
    run()
  }

  private def run(): Future[Unit] = {
    log.debug("checking for pending scheduled updates")
    dbio
      .run()
      .flatMap { scheduledCount =>
        if (scheduledCount == 0) {
          log.debug("no scheduled updates pending, trying later")
          Future {
            blocking(Thread.sleep(CHECK_INTERVAL.toMillis))
            0
          }
        } else {
          log.info(s"$scheduledCount scheduled updates processed")
          FastFuture.successful(scheduledCount)
        }
      }
      .flatMap { _ =>
        run()
      }
      .recoverWith { case ex =>
        log.error("could not process pending updates", ex)
        Future {
          blocking(Thread.sleep(BACKOFF_INTERVAL.toMillis))
        }.flatMap { _ =>
          run()
        }
      }
  }

}

class UpdateScheduler()(implicit db: Database, ec: ExecutionContext)
    extends ScheduledUpdatesRepositorySupport {

  private val dbio = new UpdateSchedulerDBIO()

  def create(ns: Namespace,
             deviceId: DeviceId,
             updateId: UpdateId,
             scheduleAt: Instant): Future[ScheduledUpdateId] =
    dbio.validateAndPersist(
      ScheduledUpdate(
        ns,
        ScheduledUpdateId.generate(),
        deviceId,
        updateId,
        scheduleAt,
        ScheduledUpdate.Status.Scheduled
      )
    )

  def cancel(ns: Namespace, scheduledUpdateId: ScheduledUpdateId): Future[Unit] =
    scheduledUpdatesRepository.setStatus(
      ns,
      scheduledUpdateId,
      ScheduledUpdate.Status.Cancelled,
      CancelledByUser.some
    )

  case object CancelledByUser extends StatusInfo

  implicit val cancelledByUserEncoder: Encoder[CancelledByUser.type] = Encoder.instance { _ =>
    Json.obj("cancelled_by_user" -> "cancelled by user".asJson)
  }

}
