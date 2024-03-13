package com.advancedtelematic.director.daemon

import akka.Done
import com.advancedtelematic.deviceregistry.common.Errors
import com.advancedtelematic.director.db.DeviceRepositorySupport
import com.advancedtelematic.libats.http.Errors.MissingEntity
import com.advancedtelematic.libats.messaging.MsgOperation.MsgOperation
import com.advancedtelematic.libats.messaging_datatype.Messages.DeleteDeviceRequest
import org.slf4j.LoggerFactory
import slick.jdbc.MySQLProfile.api.*

import scala.concurrent.{ExecutionContext, Future}

class DeleteDeviceRequestListener()(implicit val db: Database, val ec: ExecutionContext)
    extends MsgOperation[DeleteDeviceRequest]
    with DeviceRepositorySupport {

  val log = LoggerFactory.getLogger(this.getClass)

  override def apply(message: DeleteDeviceRequest): Future[Done] = {
    val f0 = db
      .run(
        com.advancedtelematic.deviceregistry.db.DeviceRepository
          .delete(message.namespace, message.uuid)
      )
      .recover { case ex @ Errors.MissingDevice =>
        log.warn("wat", ex)
      }

    val f1 = deviceRepository.markDeleted(message.namespace, message.uuid).recover {
      case e: MissingEntity[_] =>
        log.warn(
          s"error deleting device ${message.uuid} from namespace ${message.namespace}: ${e.msg}"
        )
    }

    Future.sequence(List(f0, f1)).map(_ => Done)
  }

}
