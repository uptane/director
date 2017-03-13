package com.advancedtelematic.director.manifest

import cats.syntax.either._
import com.advancedtelematic.director.client.CoreClient
import com.advancedtelematic.director.data.Codecs._
import com.advancedtelematic.director.data.DataType.{DeviceId, Namespace}
import com.advancedtelematic.director.data.DeviceRequest.{CustomManifest, DeviceManifest, OperationResult}
import com.advancedtelematic.director.db.{DeviceRepositorySupport, DeviceUpdate}
import com.advancedtelematic.director.manifest.Verifier.Verifier
import com.advancedtelematic.libtuf.data.ClientDataType.ClientKey
import com.advancedtelematic.libtuf.data.TufDataType.SignedPayload

import scala.async.Async._
import scala.concurrent.{ExecutionContext, Future}
import slick.driver.MySQLDriver.api._

class DeviceManifestUpdate(coreClient: CoreClient,
                           verifier: ClientKey => Verifier
                          )(implicit val db: Database, val ec: ExecutionContext)
  extends DeviceRepositorySupport {

  def setDeviceManifest(namespace: Namespace, device: DeviceId, signedDevMan: SignedPayload[DeviceManifest]): Future[Unit] = async {
    val ecus = await(deviceRepository.findEcus(namespace, device))
    val ecuImages = await(Future.fromTry(Verify.deviceManifest(ecus, verifier, signedDevMan)))

    deviceManifestOperationResults(signedDevMan.signed) match {
      case Nil => await(DeviceUpdate.checkAgainstTarget(namespace, device, ecuImages))
      case operations =>

        val mUpdateId = if (operations.forall(_.isSuccess)) {
          await(DeviceUpdate.checkAgainstTarget(namespace, device, ecuImages))
        } else {
          await(DeviceUpdate.clearTargets(namespace, device, ecuImages))
        }

        mUpdateId match {
          case None => ()
          case Some(updateId) =>
            await(coreClient.updateReport(namespace, device, updateId, operations))
        }
    }
  }

  private def deviceManifestOperationResults(deviceManifest: DeviceManifest): Seq[OperationResult] = {
    deviceManifest.ecu_version_manifest.flatMap(_.signed.custom.flatMap(_.as[CustomManifest].toOption))
      .map(_.operation_result)
  }
}
