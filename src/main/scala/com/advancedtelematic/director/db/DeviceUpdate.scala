package com.advancedtelematic.director.db

import com.advancedtelematic.director.data.DataType.{CustomImage, DeviceId, EcuSerial, Image, Namespace, UpdateId}
import com.advancedtelematic.director.data.DeviceRequest.EcuManifest
import com.advancedtelematic.director.http.{Errors => HttpErrors}
import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import slick.driver.MySQLDriver.api._

object DeviceUpdate extends AdminRepositorySupport
    with DeviceRepositorySupport {
  import com.advancedtelematic.libats.db.SlickAnyVal._

  private lazy val _log = LoggerFactory.getLogger(this.getClass)

  private def checkTargets[T](namespace: Namespace, device: DeviceId, next_version: Int)
                          (withTargets:  Map[EcuSerial, CustomImage] => DBIO[T])
                          (implicit db: Database, ec: ExecutionContext): DBIO[Option[UpdateId]] = {
    adminRepository.fetchTargetVersionAction(namespace, device, next_version)
      .flatMap(withTargets)
      .andThen(adminRepository.fetchUpdateIdAction(namespace, device, next_version))
  }

  private def findNextVersionOrUpdate[T](device: DeviceId)(withVersion: Int => DBIO[Option[T]])
                                     (implicit db: Database, ec: ExecutionContext): DBIO[Option[T]] = {
    deviceRepository.getNextVersionAction(device).asTry.flatMap {
      case Failure(Errors.MissingCurrentTarget) =>
        deviceRepository.updateDeviceVersionAction(device, 0).map (_ => None)
      case Failure(ex) => DBIO.failed(ex)
      case Success(next_version) => withVersion(next_version)
    }
  }

  private def updateDeviceTargetAction(namespace: Namespace, device: DeviceId, next_version: Int, translatedManifest: Map[EcuSerial, Image])
                                      (implicit db: Database, ec: ExecutionContext): DBIO[Option[UpdateId]] = {
    checkTargets(namespace, device, next_version) { targets =>
      val translatedTargets = targets.mapValues(_.image)
      if (translatedTargets == translatedManifest) {
        deviceRepository.updateDeviceVersionAction(device, next_version)
      } else {
        _log.info {
          s"""version : $next_version
             |targets : $translatedTargets
             |manifest: $translatedManifest
           """.stripMargin
        }
        _log.error(s"Device $device updated to the wrong target")
        DBIO.failed(HttpErrors.DeviceUpdatedToWrongTarget)
      }
    }
  }

  def checkAgainstTarget(namespace: Namespace, device: DeviceId, ecuImages: Seq[EcuManifest])
                        (implicit db: Database, ec: ExecutionContext): Future[Option[UpdateId]] = {
    val translatedManifest = ecuImages.map(ecu => (ecu.ecu_serial, ecu.installed_image)).toMap

    val dbAct = setEcusAction(namespace, device, ecuImages){
      findNextVersionOrUpdate(device) { next_version =>
        updateDeviceTargetAction(namespace, device, next_version, translatedManifest)
      }
    }

    db.run(dbAct.transactionally)
  }

  protected [db] def clearTargetsFrom(namespace: Namespace, device: DeviceId, version: Int)
                                     (implicit db: Database, ec: ExecutionContext): DBIO[Unit] = {
    val clearRequest = Schema.fileCacheRequest
      .filter(_.namespace === namespace)
      .filter(_.device === device)
      .filter(_.version >= version)
      .delete

    val clearCache = Schema.fileCache
      .filter(_.device === device)
      .filter(_.version >= version)
      .delete

    val setVersion = Schema.deviceTargets
      .filter(_.device === device)
      .filter(_.version >= version)
      .delete

    clearRequest.andThen(clearCache).andThen(setVersion).map(_ => ()).transactionally
  }

  def clearTargets(namespace: Namespace, device: DeviceId, ecuImages: Seq[EcuManifest])
                  (implicit db: Database, ec: ExecutionContext): Future[Option[UpdateId]] = {
    val dbAct = for {
        _ <- setEcusAction(namespace, device, ecuImages)(DBIO.successful(None))
        next_version <- deviceRepository.getNextVersionAction(device)
        updateId <- adminRepository.fetchUpdateIdAction(namespace, device, next_version)
        _ <- clearTargetsFrom(namespace, device, next_version)
      } yield updateId

    db.run(dbAct.transactionally)
  }

  protected [db] def setEcusAction[T](namespace: Namespace, device: DeviceId, ecuImages: Seq[EcuManifest])(ifNotSame : DBIO[Option[T]])
                                  (implicit db: Database, ec: ExecutionContext): DBIO[Option[T]] = {
    val translatedManifest = ecuImages.map(ecu => (ecu.ecu_serial, ecu.installed_image)).toMap

    adminRepository.findImagesAction(namespace, device).flatMap { currentStored =>
      if (currentStored.toMap == translatedManifest) {
        DBIO.successful(None)
      } else {
        deviceRepository.persistAllAction(ecuImages).andThen(ifNotSame)
      }
    }
  }
}
