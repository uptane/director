package com.advancedtelematic.director.repo

import com.advancedtelematic.director.data.DataType.AdminRoleName
import com.advancedtelematic.director.data.DbDataType.SignedPayloadToDbRole
import com.advancedtelematic.director.db.DbOfflineUpdatesRepositorySupportSupport
import com.advancedtelematic.director.http.Errors
import com.advancedtelematic.libtuf.data.ClientDataType.{ClientTargetItem, OfflineSnapshotRole, OfflineUpdatesRole, TufRole, TufRoleOps}
import com.advancedtelematic.libtuf.data.TufDataType.{JsonSignedPayload, RepoId, RoleType, TargetFilename, TargetName}
import slick.jdbc.MySQLProfile.api._
import com.advancedtelematic.libtuf.data.ClientCodecs._
import com.advancedtelematic.libtuf_server.keyserver.KeyserverClient
import com.advancedtelematic.libtuf_server.repo.server.DataType.SignedRole
import io.circe.syntax._
import com.advancedtelematic.libtuf.data.ClientDataType.TufRole._
import io.circe.Codec

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}
import scala.async.Async._

class OfflineUpdates(keyserverClient: KeyserverClient)(implicit val db: Database, val ec: ExecutionContext) extends DbOfflineUpdatesRepositorySupportSupport {

  private val defaultExpire = Duration.ofDays(365)

  private val DEFAULT_SNAPSHOTS_NAME = AdminRoleName("offline-snapshots")

  def findLatestTargets(repoId: RepoId, name: AdminRoleName): Future[JsonSignedPayload] =
    findFreshLatest[OfflineUpdatesRole](repoId, name)

  def findLatestSnapshot(repoId: RepoId): Future[JsonSignedPayload] =
    findFreshLatest[OfflineSnapshotRole](repoId, DEFAULT_SNAPSHOTS_NAME)

  private def findFreshLatest[T : Codec](repoId: RepoId, name: AdminRoleName)(implicit tufRole: TufRole[T]): Future[JsonSignedPayload] = async {
    val existing = await(dbAdminRolesRepository.findLatest(repoId, tufRole.roleType, name))

    if (existing.isExpired) {
      val versionedRole = existing.toSignedRole[T]
      val newRole = versionedRole.tufRole.refreshRole(versionedRole.role, _ + 1, nextExpires)

      if(tufRole.roleType == RoleType.OFFLINE_UPDATES) { // This case needs to trigger a snapshot refresh
        val (signedTargets, _) = await(signAndPersistWithSnapshot(repoId, name, newRole.asInstanceOf[OfflineUpdatesRole]))
        signedTargets.content
      } else {
        val signed = await(sign(repoId, newRole))
        await(dbAdminRolesRepository.persistAll(signed.toDbAdminRole(repoId, name)))
        signed.content
      }

    } else
      existing.content
  }

  def addOrReplace(repoId: RepoId, offlineTargetsName: AdminRoleName, targetName: TargetFilename, target: ClientTargetItem): Future[Unit] = async {
    val existing = await(dbAdminRolesRepository.findLatestOpt(repoId, RoleType.OFFLINE_UPDATES, offlineTargetsName))

    val targets = Map(targetName -> target)

    val newRole = if(existing.isEmpty) {
      await(keyserverClient.addOfflineUpdatesRole(repoId)) // If there is no previous targets, create the role first
      OfflineUpdatesRole(targets, expires = nextExpires, version = 1)
    } else {
      val role = existing.get.toSignedRole[OfflineUpdatesRole].role
      val newTargets = role.targets ++ targets
      val newRole = role.copy(targets = newTargets, expires = nextExpires, version = role.version + 1)
      newRole
    }

    await(signAndPersistWithSnapshot(repoId, offlineTargetsName, newRole))
  }

  private def nextExpires = Instant.now().plus(defaultExpire)

  def delete(repoId: RepoId, offlineTargetsName: AdminRoleName, targetName: TargetFilename): Future[Unit] = async {
    val existing = await(dbAdminRolesRepository.findLatest(repoId, RoleType.OFFLINE_UPDATES, offlineTargetsName))
    val role = existing.toSignedRole[OfflineUpdatesRole].role

    if(!role.targets.contains(targetName))
      throw Errors.MissingTarget(repoId, targetName)

    val newTargets = role.targets - targetName
    val newRole = role.copy(newTargets, version = role.version + 1, expires = nextExpires)

    await(signAndPersistWithSnapshot(repoId, offlineTargetsName, newRole))
  }

  private def signAndPersistWithSnapshot(repoId: RepoId, name: AdminRoleName, targets: OfflineUpdatesRole): Future[(SignedRole[OfflineUpdatesRole], SignedRole[OfflineSnapshotRole])] = async {
    val oldSnapshotsO = await(dbAdminRolesRepository.findLatestOpt(repoId, RoleType.OFFLINE_SNAPSHOT, DEFAULT_SNAPSHOTS_NAME))
    val nextVersion = oldSnapshotsO.map(_.version).getOrElse(0) + 1

    val savedRolesMeta = await(dbAdminRolesRepository.findAll(repoId, RoleType.OFFLINE_UPDATES)).map { adminRole =>
      val (_, metaItem) = adminRole.toSignedRole[OfflineUpdatesRole].asMetaRole
      name.asMetaPath -> metaItem
    }.toMap

    val signedTargets = await(sign(repoId, targets))

    val (_, metaItem) = signedTargets.asMetaRole
    val newRolesMeta = savedRolesMeta + (name.asMetaPath -> metaItem)

    val newSnapshots = OfflineSnapshotRole(newRolesMeta, nextExpires, version = nextVersion)
    val signedSnapshots = await(sign(repoId, newSnapshots))

    await(dbAdminRolesRepository.persistAll(signedTargets.toDbAdminRole(repoId, name), signedSnapshots.toDbAdminRole(repoId, DEFAULT_SNAPSHOTS_NAME)))

    (signedTargets, signedSnapshots)
  }

  private def sign[T : Codec : TufRole](repoId: RepoId, role: T): Future[SignedRole[T]] = async {
    val signedPayload = await(keyserverClient.sign(repoId, role))
    await(SignedRole.withChecksum[T](signedPayload.asJsonSignedPayload, role.version, role.expires))
  }
}
