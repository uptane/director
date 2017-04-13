package com.advancedtelematic.director.db

import com.advancedtelematic.director.data.DataType.{DeviceId, Ecu, EcuTarget, HardwareIdentifier, MultiTargetUpdate, UpdateId}
import com.advancedtelematic.director.data.DataType
import com.advancedtelematic.libats.data.Namespace
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libtuf.data.TufDataType.{Checksum, RepoId, RoleType}
import io.circe.Json

import scala.concurrent.{ExecutionContext, Future}
import slick.driver.MySQLDriver.api._

import scala.util.{Failure, Success}
import Errors._
import com.advancedtelematic.libats.data.Namespace
import com.advancedtelematic.libtuf.data.ClientDataType.TargetFilename
import com.advancedtelematic.libats.slick.db.SlickUUIDKey._

trait AdminRepositorySupport {
  def adminRepository(implicit db: Database, ec: ExecutionContext) = new AdminRepository()
}

protected class AdminRepository()(implicit db: Database, ec: ExecutionContext) {
  import com.advancedtelematic.director.data.AdminRequest.{EcuInfoResponse, EcuInfoImage, RegisterEcu}
  import com.advancedtelematic.director.data.DataType.{CustomImage, DeviceUpdateTarget, EcuSerial, Image, UpdateId}
  import com.advancedtelematic.libats.slick.db.SlickExtensions._
  import com.advancedtelematic.libats.slick.db.SlickAnyVal._
  import com.advancedtelematic.libats.slick.codecs.SlickRefined._
  import com.advancedtelematic.libtuf.data.ClientDataType.ClientKey
  import com.advancedtelematic.libtuf.data.TufSlickMappings._

  implicit private class NotInCampaign(query: Query[Rep[DeviceId], DeviceId, Seq]) {
    def devTargets = Schema.deviceTargets
      .groupBy(_.device)
      .map{case (devId, q) => (devId, q.map(_.version).max.getOrElse(0))}

    def notInACampaign: Query[Rep[DeviceId], DeviceId, Seq] = query
      .join(Schema.deviceCurrentTarget).on(_ === _.device)
      .map{case (devId, devCurTarget) => (devId, devCurTarget.deviceCurrentTarget)}
      .joinLeft(devTargets).on(_._1 === _._1)
      .map{case ((devId, curTarg), devUpdate) => (devId, curTarg, devUpdate.map(_._2).getOrElse(curTarg))}
      .filter{ case(_, cur, lastScheduled) => cur === lastScheduled}
      .map(_._1)
  }

  private def byDevice(namespace: Namespace, device: DeviceId): Query[Schema.EcusTable, Ecu, Seq] =
    Schema.ecu
      .filter(_.namespace === namespace)
      .filter(_.device === device)

  protected [db] def fetchHwMappingAction(namespace: Namespace, device: DeviceId): DBIO[Map[EcuSerial, HardwareIdentifier]] =
    byDevice(namespace, device)
      .map(x => x.ecuSerial -> x.hardwareId)
      .result
      .failIfEmpty(DeviceMissing)
      .map(_.toMap)

  protected [db] def findImagesAction(namespace: Namespace, device: DeviceId): DBIO[Seq[(EcuSerial, Image)]] =
    byDevice(namespace, device)
      .map(_.ecuSerial)
      .join(Schema.currentImage).on(_ === _.id)
      .map(_._2)
      .result
      .map(_.map(cim => cim.ecuSerial -> cim.image))

  def findImages(namespace: Namespace, device: DeviceId): Future[Seq[(EcuSerial, Image)]] = db.run {
    findImagesAction(namespace, device)
  }

  def findAffected(namespace: Namespace, filepath: TargetFilename, offset: Long, limit: Long): Future[PaginationResult[DeviceId]] = db.run {
    Schema.currentImage
      .filter(_.filepath === filepath)
      .map(_.id)
      .join(Schema.ecu.filter(_.namespace === namespace)).on(_ === _.ecuSerial)
      .map(_._2)
      .map(_.device)
      .notInACampaign
      .distinct
      .paginateResult(offset = offset, limit = limit)
  }

  def findDevice(namespace: Namespace, device: DeviceId): Future[Seq[EcuInfoResponse]] = db.run {
    val query: Rep[Seq[(EcuSerial,HardwareIdentifier,Boolean,TargetFilename,Long,Checksum)]] = for {
      ecu <- Schema.ecu if ecu.namespace === namespace && ecu.device === device
      curImage <- Schema.currentImage if ecu.ecuSerial === curImage.id
    } yield (ecu.ecuSerial, ecu.hardwareId, ecu.primary, curImage.filepath, curImage.length, curImage.checksum)

    for {
      devices <- query.result.failIfEmpty(MissingDevice)
    } yield for {
      (id, hardwareId, primary, filepath, size, checksum) <- devices
    } yield EcuInfoResponse(id, hardwareId, primary, EcuInfoImage(filepath, size, Map(checksum.method -> checksum.hash)))
  }

  def findPublicKey(namespace: Namespace, device: DeviceId, ecu_serial: EcuSerial): Future[ClientKey] = db.run {
    Schema.ecu
      .filter(_.namespace === namespace)
      .filter(_.device === device)
      .filter(_.ecuSerial === ecu_serial)
      .map(x => (x.cryptoMethod, x.publicKey))
      .result
      .failIfNotSingle(MissingEcu)
      .map{case (typ, key) => ClientKey(typ, key)}
  }

  def createDevice(namespace: Namespace, device: DeviceId, primEcu: EcuSerial, ecus: Seq[RegisterEcu]): Future[Unit] = {
    val toClean = byDevice(namespace, device)
    val clean = Schema.currentImage.filter(_.id in toClean.map(_.ecuSerial)).delete.andThen(toClean.delete)

    def register(reg: RegisterEcu) =
      Schema.ecu += Ecu(reg.ecu_serial, device, namespace, reg.ecu_serial == primEcu, reg.hardwareId, reg.clientKey)

    val act = clean.andThen(DBIO.sequence(ecus.map(register)))

    db.run(act.map(_ => ()).transactionally)
  }

  protected [db] def getLatestVersion(namespace: Namespace, device: DeviceId): DBIO[Int] =
    Schema.deviceTargets
      .filter(_.device === device)
      .map(_.version)
      .max
      .result
      .failIfNone(NoTargetsScheduled)

  protected [db] def fetchUpdateIdAction(namespace: Namespace, device: DeviceId, version: Int): DBIO[Option[UpdateId]] =
    Schema.deviceTargets
      .filter(_.device === device)
      .filter(_.version === version)
      .map(_.update)
      .result
      .failIfMany()
      .map(_.flatten)

  protected [db] def fetchTargetVersionAction(namespace: Namespace, device: DeviceId, version: Int): DBIO[Map[EcuSerial, CustomImage]] =
    Schema.ecu
      .filter(_.namespace === namespace)
      .filter(_.device === device)
      .join(Schema.ecuTargets.filter(_.version === version)).on(_.ecuSerial === _.id)
      .map(_._2)
      .result
      .map(_.groupBy(_.ecuIdentifier).mapValues(_.head.image))

  def fetchTargetVersion(namespace: Namespace, device: DeviceId, version: Int): Future[Map[EcuSerial, CustomImage]] =
    db.run(fetchTargetVersionAction(namespace, device, version))

  protected [db] def storeTargetVersion(namespace: Namespace, device: DeviceId, updateId: Option[UpdateId],
                                        version: Int, targets: Map[EcuSerial, CustomImage]): DBIO[Unit] = {
    val act = (Schema.ecuTargets
      ++= targets.map{ case (ecuSerial, image) => EcuTarget(version, ecuSerial, image)})

    val updateDeviceTargets = Schema.deviceTargets += DeviceUpdateTarget(device, updateId, version)

    act.andThen(updateDeviceTargets).map(_ => ()).transactionally
  }

  protected [db] def updateTargetAction(namespace: Namespace, device: DeviceId, updateId: Option[UpdateId], targets: Map[EcuSerial, CustomImage]): DBIO[Int] = for {
    version <- getLatestVersion(namespace, device).asTry.flatMap {
      case Success(x) => DBIO.successful(x)
      case Failure(NoTargetsScheduled) => DBIO.successful(0)
      case Failure(ex) => DBIO.failed(ex)
    }
    previousMap <- fetchTargetVersionAction(namespace, device, version)
    new_version = version + 1
    new_targets = previousMap ++ targets
    _ <- storeTargetVersion(namespace, device, updateId, new_version, new_targets)
  } yield new_version

  def updateTarget(namespace: Namespace, device: DeviceId, updateId: Option[UpdateId], targets: Map[EcuSerial, CustomImage]): Future[Int] = db.run {
    updateTargetAction(namespace, device, updateId, targets).transactionally
  }

  def getPrimaryEcuForDevice(device: DeviceId): Future[EcuSerial] = db.run {
    Schema.ecu
      .filter(_.device === device)
      .filter(_.primary)
      .map(_.ecuSerial)
      .result
      .failIfNotSingle(DeviceMissingPrimaryEcu)
  }

  def getUpdatesFromTo(namespace: Namespace, device: DeviceId,
                       fromVersion: Int, toVersion: Int): Future[Seq[Option[UpdateId]]] = db.run {
    Schema.deviceTargets
      .filter(_.device === device)
      .filter(_.version > fromVersion)
      .filter(_.version <= toVersion)
      .map(x => (x.version, x.update))
      .sortBy(_._1)
      .map(_._2)
      .result
  }

}

trait DeviceRepositorySupport {
  def deviceRepository(implicit db: Database, ec: ExecutionContext) = new DeviceRepository()
}

protected class DeviceRepository()(implicit db: Database, ec: ExecutionContext) {
  import com.advancedtelematic.director.data.AdminRequest.RegisterEcu
  import com.advancedtelematic.director.data.DeviceRequest.EcuManifest
  import com.advancedtelematic.libats.slick.db.SlickExtensions._
  import com.advancedtelematic.libats.slick.db.SlickAnyVal._
  import com.advancedtelematic.libats.slick.codecs.SlickRefined._
  import DataType.{CurrentImage, DeviceCurrentTarget, EcuSerial}

  private def byDevice(namespace: Namespace, device: DeviceId): Query[Schema.EcusTable, Ecu, Seq] =
    Schema.ecu
      .filter(_.namespace === namespace)
      .filter(_.device === device)

  private def persistEcu(ecuManifest: EcuManifest): DBIO[Unit] = {
    Schema.currentImage.insertOrUpdate(CurrentImage(ecuManifest.ecu_serial, ecuManifest.installed_image, ecuManifest.attacks_detected)).map(_ => ())
  }

  protected [db] def persistAllAction(ecuManifests: Seq[EcuManifest]): DBIO[Unit] =
    DBIO.sequence(ecuManifests.map(persistEcu)).map(_ => ()).transactionally

  def persistAll(ecuManifests: Seq[EcuManifest]): Future[Unit] =
    db.run(persistAllAction(ecuManifests))

  def create(namespace: Namespace, device: DeviceId, primEcu: EcuSerial, ecus: Seq[RegisterEcu]): Future[Unit] = {
    def register(reg: RegisterEcu) =
      Schema.ecu += Ecu(reg.ecu_serial, device, namespace, reg.ecu_serial == primEcu, reg.hardwareId, reg.clientKey)

    val dbAct = byDevice(namespace, device).exists.result.flatMap {
      case false => DBIO.sequence(ecus.map(register)).map(_ => ())
      case true  => DBIO.failed(DeviceAlreadyRegistered)
    }

    db.run(dbAct.transactionally)
  }

  def findEcus(namespace: Namespace, device: DeviceId): Future[Seq[Ecu]] =
    db.run(byDevice(namespace, device).result)

  protected [db] def getCurrentVersionAction(device: DeviceId): DBIO[Int] =
    Schema.deviceCurrentTarget
      .filter(_.device === device)
      .map(_.deviceCurrentTarget)
      .result
      .failIfNotSingle(MissingCurrentTarget)

  def getCurrentVersion(device: DeviceId): Future[Int] = db.run(getCurrentVersionAction(device))

  protected [db] def getNextVersionAction(device: DeviceId): DBIO[Int] = {
    val devVer = getCurrentVersionAction(device)

    val targetVer = Schema.deviceTargets
      .filter(_.device === device)
      .map(_.version)
      .max
      .result
      .failIfNone(NoTargetsScheduled)

    devVer.zip(targetVer).map { case (device_version, target_version) =>
      scala.math.min(device_version + 1, target_version)
    }
  }

  def getNextVersion(device: DeviceId): Future[Int] = db.run(getNextVersionAction(device))

  protected [db] def updateDeviceVersionAction(device: DeviceId, device_version: Int): DBIO[Unit] = {
    Schema.deviceCurrentTarget.insertOrUpdate(DeviceCurrentTarget(device, device_version))
      .map(_ => ())
  }

}

trait FileCacheRepositorySupport {
  def fileCacheRepository(implicit db: Database, ec: ExecutionContext) = new FileCacheRepository()
}

protected class FileCacheRepository()(implicit db: Database, ec: ExecutionContext) {
  import com.advancedtelematic.libats.slick.db.SlickExtensions._
  import com.advancedtelematic.libtuf.data.ClientCodecs._
  import com.advancedtelematic.libtuf.data.ClientDataType.{SnapshotRole, TargetsRole, TimestampRole}
  import com.advancedtelematic.libats.slick.db.SlickCirceMapper.jsonMapper
  import com.advancedtelematic.libtuf.data.TufCodecs._
  import com.advancedtelematic.libtuf.data.TufDataType.SignedPayload
  import io.circe.syntax._
  import DataType.FileCache

  private def fetchRoleType(role: RoleType.RoleType, err: => Throwable)(device: DeviceId, version: Int): Future[Json] = db.run {
    Schema.fileCache
      .filter(_.role === role)
      .filter(_.version === version)
      .filter(_.device === device)
      .map(_.fileEntity)
      .result
      .failIfNotSingle(err)
  }

  def fetchTarget(device: DeviceId, version: Int): Future[Json] = fetchRoleType(RoleType.TARGETS, MissingTarget)(device, version)

  def fetchSnapshot(device: DeviceId, version: Int): Future[Json] = fetchRoleType(RoleType.SNAPSHOT, MissingSnapshot)(device, version)

  def fetchTimestamp(device: DeviceId, version: Int): Future[Json] = fetchRoleType(RoleType.TIMESTAMP, MissingTimestamp)(device, version)

  protected [db] def storeRoleTypeAction(role: RoleType.RoleType, err: => Throwable)(device: DeviceId, version: Int, file: Json): DBIO[Unit] =
    (Schema.fileCache += FileCache(role, version, device, file))
      .handleIntegrityErrors(err)
      .map(_ => ())

  private def storeTargetsAction(device: DeviceId, version: Int, file: Json): DBIO[Unit] =
    storeRoleTypeAction(RoleType.TARGETS, ConflictingTarget)(device, version, file)

  private def storeSnapshotAction(device: DeviceId, version: Int, file: Json): DBIO[Unit] =
    storeRoleTypeAction(RoleType.SNAPSHOT, ConflictingSnapshot)(device, version, file)

  private def storeTimestampAction(device: DeviceId, version: Int, file: Json): DBIO[Unit] =
    storeRoleTypeAction(RoleType.TIMESTAMP, ConflictingTimestamp)(device, version, file)

  def storeJson(device: DeviceId, version: Int, targets: SignedPayload[TargetsRole],
                snapshots: SignedPayload[SnapshotRole], timestamp: SignedPayload[TimestampRole]): Future[Unit] = db.run {
    storeTargetsAction(device, version, targets.asJson)
      .andThen(storeSnapshotAction(device, version, snapshots.asJson))
      .andThen(storeTimestampAction(device, version, timestamp.asJson))
      .transactionally
  }
}


trait FileCacheRequestRepositorySupport {
  def fileCacheRequestRepository(implicit db: Database, ec: ExecutionContext) = new FileCacheRequestRepository()
}

protected class FileCacheRequestRepository()(implicit db: Database, ec: ExecutionContext) {
  import com.advancedtelematic.director.data.FileCacheRequestStatus._
  import com.advancedtelematic.libats.slick.db.SlickExtensions._
  import com.advancedtelematic.libats.slick.db.SlickAnyVal._
  import DataType.FileCacheRequest

  protected [db] def persistAction(req: FileCacheRequest): DBIO[Unit] =
    (Schema.fileCacheRequest += req)
      .map(_ => ())
      .handleIntegrityErrors(ConflictingFileCacheRequest)

  def persist(req: FileCacheRequest): Future[Unit] = db.run {
    persistAction(req)
  }

  def findPending(limit: Int = 10): Future[Seq[FileCacheRequest]] = db.run {
    Schema.fileCacheRequest.filter(_.status === PENDING).take(limit).result
  }

  def updateRequest(req: FileCacheRequest): Future[Unit] = db.run {
    Schema.fileCacheRequest
      .filter(_.namespace === req.namespace)
      .filter(_.timestampVersion === req.timestampVersion)
      .filter(_.device === req.device)
      .map(_.status)
      .update(req.status)
      .handleSingleUpdateError(MissingFileCacheRequest)
      .map(_ => ())
  }
}

trait RepoNameRepositorySupport {
  def repoNameRepository(implicit db: Database, ec: ExecutionContext) = new RepoNameRepository()
}

protected class RepoNameRepository()(implicit db: Database, ec: ExecutionContext) {
  import DataType.RepoName
  import com.advancedtelematic.libats.slick.db.SlickAnyVal._
  import com.advancedtelematic.libats.slick.db.SlickExtensions._
  import com.advancedtelematic.libats.slick.db.SlickUUIDKey._

  def getRepo(ns: Namespace): Future[RepoId] = db.run {
    Schema.repoNames
      .filter(_.ns === ns)
      .map(_.repo)
      .result
      .failIfNotSingle(MissingNamespaceRepo)
  }

  protected [db] def persistAction(ns: Namespace, repoId: RepoId): DBIO[RepoName] = {
    val repoName = RepoName(ns, repoId)
    (Schema.repoNames += repoName)
      .handleIntegrityErrors(ConflictNamespaceRepo)
      .map(_ => repoName)
  }
}

trait RootFilesRepositorySupport {
  def rootFilesRepository(implicit db: Database, ec: ExecutionContext) = new RootFilesRepository()
}

protected class RootFilesRepository()(implicit db: Database, ec: ExecutionContext) extends RepoNameRepositorySupport {
  import DataType.RootFile
  import com.advancedtelematic.libats.slick.db.SlickAnyVal._
  import com.advancedtelematic.libats.slick.db.SlickExtensions._
  import com.advancedtelematic.libats.slick.db.SlickCirceMapper._
  import com.advancedtelematic.libats.slick.db.SlickPipeToUnit.pipeToUnit

  def find(ns: Namespace): Future[Json] = db.run {
    Schema.rootFiles
      .filter(_.namespace === ns)
      .map(_.root)
      .result
      .failIfNotSingle(MissingRootFile)
  }

  protected [db] def persistAction(ns: Namespace, rootFile: Json): DBIO[RootFile] = {
    val root = RootFile(ns, rootFile)
    (Schema.rootFiles += root)
      .handleIntegrityErrors(ConflictingRootFile)
      .map(_ => root)
  }

  def persistNamespaceRootFile(namespace: Namespace,
                               rootFile: Json,
                               repoId: RepoId): Future[Unit] = db.run {
    persistAction(namespace, rootFile)
      .andThen(repoNameRepository.persistAction(namespace, repoId))
      .transactionally
  }
}

trait MultiTargetUpdatesRepositorySupport {
  def multiTargetUpdatesRepository(implicit db: Database, ec: ExecutionContext) = new MultiTargetUpdatesRepository()
}

protected class MultiTargetUpdatesRepository()(implicit db: Database, ec: ExecutionContext) {
  import com.advancedtelematic.libats.slick.db.SlickExtensions._
  import com.advancedtelematic.libats.slick.codecs.SlickRefined._
  import com.advancedtelematic.libats.slick.db.SlickAnyVal._

  protected [db] def fetchAction(id: UpdateId, ns: Namespace): DBIO[Seq[MultiTargetUpdate]] =
    Schema.multiTargets
      .filter(_.id === id)
      .filter(_.namespace === ns)
      .result
      .failIfEmpty(MissingMultiTargetUpdate)

  def fetch(id: UpdateId, ns: Namespace): Future[Seq[MultiTargetUpdate]] = db.run {
    fetchAction(id, ns)
  }

  def create(rows: Seq[MultiTargetUpdate]): Future[Unit] = db.run {
    (Schema.multiTargets ++= rows)
      .handleIntegrityErrors(ConflictingMultiTargetUpdate)
      .map(_ => ())
  }
}

trait LaunchedMultiTargetUpdateRepositorySupport {
  def launchedMultiTargetUpdateRepository(implicit db: Database, ec: ExecutionContext) = new LaunchedMultiTargetUpdateRepository()
}

protected class LaunchedMultiTargetUpdateRepository()(implicit db: Database, ec: ExecutionContext) {
  import com.advancedtelematic.director.data.DataType.LaunchedMultiTargetUpdate
  import com.advancedtelematic.director.data.LaunchedMultiTargetUpdateStatus
  import com.advancedtelematic.libats.slick.db.SlickExtensions._
  import com.advancedtelematic.libats.slick.db.SlickUUIDKey._
  import Schema.launchedMultiTargetUpdates

  protected [db] def persistAction(lmtu: LaunchedMultiTargetUpdate): DBIO[LaunchedMultiTargetUpdate] =
    (launchedMultiTargetUpdates += lmtu)
      .handleIntegrityErrors(ConflictingLaunchedMultiTargetUpdate)
      .map(_ => lmtu)

  def persist(lmtu: LaunchedMultiTargetUpdate): Future[LaunchedMultiTargetUpdate] = db.run(persistAction(lmtu))

  def setStatus(device: DeviceId, updateId: UpdateId, timestampVersion: Int,
                status: LaunchedMultiTargetUpdateStatus.Status): Future[Unit] = db.run {
    launchedMultiTargetUpdates
      .filter(_.device === device)
      .filter(_.update === updateId)
      .filter(_.timestampVersion === timestampVersion)
      .map(_.status)
      .update(status)
      .handleSingleUpdateError(MissingLaunchedMultiTargetUpdate)
  }
}

trait UpdateTypesRepositorySupport {
  def updateTypesRepository(implicit db: Database, ec: ExecutionContext) = new UpdateTypesRepository()
}

protected class UpdateTypesRepository()(implicit db: Database, ec: ExecutionContext) {
  import com.advancedtelematic.director.data.UpdateType.UpdateType
  import com.advancedtelematic.libats.slick.db.SlickExtensions._

  protected [db] def persistAction(updateId: UpdateId, ofType: UpdateType): DBIO[Unit] =
    (Schema.updateTypes += ((updateId, ofType)))
      .handleIntegrityErrors(ConflictingUpdateType)
      .map(_ => ())

  def getType(updateId: UpdateId): Future[UpdateType] = db.run {
    Schema.updateTypes
      .filter(_.update === updateId)
      .map(_.updateType)
      .result
      .failIfNotSingle(MissingUpdateType)
  }
}

