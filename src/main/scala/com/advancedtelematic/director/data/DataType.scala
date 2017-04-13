package com.advancedtelematic.director.data

import akka.http.scaladsl.model.Uri
import com.advancedtelematic.libats.codecs.CirceEnum
import com.advancedtelematic.libats.data.Namespace
import com.advancedtelematic.libtuf.data.ClientDataType.{ClientKey, TargetFilename, ClientHashes => Hashes}
import com.advancedtelematic.libtuf.data.TufDataType.{Checksum, RepoId, RoleType}
import eu.timepit.refined.api.{Refined, Validate}
import io.circe.Json
import java.util.UUID

import com.advancedtelematic.libats.data.UUIDKey.{UUIDKey, UUIDKeyObj}
import com.advancedtelematic.libats.slick.codecs.SlickEnum

object FileCacheRequestStatus extends CirceEnum with SlickEnum {
  type Status = Value

  val SUCCESS, ERROR, PENDING = Value
}

object LaunchedMultiTargetUpdateStatus extends CirceEnum with SlickEnum {
  type Status = Value

  val Pending, InFlight, Canceled, Failed, Finished = Value
}

object UpdateType extends CirceEnum with SlickEnum {
  type UpdateType = Value

  val OLD_STYLE_CAMPAIGN, MULTI_TARGET_UPDATE = Value
}

object DataType {
  import RoleType.RoleType

  final case class DeviceId(uuid: UUID) extends UUIDKey
  object DeviceId extends UUIDKeyObj[DeviceId]

  final case class UpdateId(uuid: UUID) extends UUIDKey
  object UpdateId extends UUIDKeyObj[UpdateId]

  final case class ValidEcuSerial()
  type EcuSerial = Refined[String, ValidEcuSerial]
  implicit val validEcuSerial: Validate.Plain[String, ValidEcuSerial] = ValidationUtils.validInBetween(min = 1, max = 64, ValidEcuSerial())

  final case class ValidHardwareIdentifier()
  type HardwareIdentifier = Refined[String, ValidHardwareIdentifier]
  implicit val validHardwareIdentifier: Validate.Plain[String, ValidHardwareIdentifier] = ValidationUtils.validInBetween(min = 0, max = 200, ValidHardwareIdentifier())

  final case class FileInfo(hashes: Hashes, length: Long)
  final case class Image(filepath: TargetFilename, fileinfo: FileInfo)

  final case class CustomImage(filepath: TargetFilename, fileinfo: FileInfo, uri: Uri) {
    def image: Image = Image(filepath, fileinfo)
  }

  object CustomImage {
    def apply(img: Image, uri: Uri): CustomImage = CustomImage(img.filepath, img.fileinfo, uri)
  }

  final case class Ecu(ecuSerial: EcuSerial, device: DeviceId, namespace: Namespace, primary: Boolean,
                       hardwareId: HardwareIdentifier, clientKey: ClientKey)

  final case class CurrentImage (ecuSerial: EcuSerial, image: Image, attacksDetected: String)

  final case class EcuTarget(version: Int, ecuIdentifier: EcuSerial, image: CustomImage)

  final case class DeviceUpdateTarget(device: DeviceId, updateId: Option[UpdateId], targetVersion: Int)

  final case class DeviceCurrentTarget(device: DeviceId, targetVersion: Int)

  final case class FileCache(role: RoleType, version: Int, device: DeviceId, file: Json)

  final case class FileCacheRequest(namespace: Namespace, targetVersion: Int, device: DeviceId,
                                    status: FileCacheRequestStatus.Status, timestampVersion: Int)

  final case class RepoName(namespace: Namespace, repoId: RepoId)

  final case class RootFile(namespace: Namespace, rootFile: Json)

  final case class MultiTargetUpdate(id: UpdateId, hardwareId: HardwareIdentifier, target: TargetFilename, checksum: Checksum,
                                     targetLength: Long, namespace: Namespace) {
    lazy val image: Image = {
      val clientHash = Map(checksum.method -> checksum.hash)
      Image(target, FileInfo(clientHash, targetLength))
    }
  }

  object MultiTargetUpdate {
    def apply(mtu: MultiTargetUpdateRequest, id: UpdateId, namespace: Namespace): Seq[MultiTargetUpdate] =
      mtu.targets.toSeq.map {case (hardwareId, target) =>
        MultiTargetUpdate(id = id, hardwareId = hardwareId, target = target.target,
                          checksum = target.checksum, targetLength = target.targetLength,
                          namespace = namespace)
      }
  }

  final case class TargetUpdate(target: TargetFilename, checksum: Checksum, targetLength: Long) {
    lazy val image: Image = {
      val clientHash = Map(checksum.method -> checksum.hash)
      Image(target, FileInfo(clientHash, targetLength))
    }
  }

  final case class MultiTargetUpdateRequest(targets: Map[HardwareIdentifier, TargetUpdate])

  final case class LaunchedMultiTargetUpdate(device: DeviceId, update: UpdateId, timestampVersion: Int,
                                             status: LaunchedMultiTargetUpdateStatus.Status)

}
