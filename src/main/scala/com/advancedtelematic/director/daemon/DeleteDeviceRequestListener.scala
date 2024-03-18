package com.advancedtelematic.director.daemon

import akka.Done
import com.advancedtelematic.director.deviceregistry.common.Errors
import com.advancedtelematic.director.db.ProvisionedDeviceRepositorySupport
import com.advancedtelematic.director.db.deviceregistry.DeviceRepository
import com.advancedtelematic.libats.http.Errors.MissingEntity
import com.advancedtelematic.libats.messaging.MsgOperation.MsgOperation
import com.advancedtelematic.libats.messaging_datatype.Messages.DeleteDeviceRequest
import org.slf4j.LoggerFactory
import slick.jdbc.MySQLProfile.api.*

import scala.concurrent.{ExecutionContext, Future}

class DeleteDeviceRequestListener()(implicit val db: Database, val ec: ExecutionContext)
    extends MsgOperation[DeleteDeviceRequest]
    with ProvisionedDeviceRepositorySupport {

  val log = LoggerFactory.getLogger(this.getClass)

  override def apply(message: DeleteDeviceRequest): Future[Done] = {
    val f0 = db
      .run(
        DeviceRepository
          .delete(message.namespace, message.uuid)
      )
      .recover { case ex @ Errors.MissingDevice =>
        log.warn("wat", ex)
      }

    val f1 = provisionedDeviceRepository.markDeleted(message.namespace, message.uuid).recover {
      case e: MissingEntity[_] =>
        log.warn(
          s"error deleting device ${message.uuid} from namespace ${message.namespace}: ${e.msg}"
        )
    }

    Future.sequence(List(f0, f1)).map(_ => Done)
  }

}
