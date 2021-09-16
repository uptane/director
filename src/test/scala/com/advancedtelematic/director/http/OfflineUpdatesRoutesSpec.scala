package com.advancedtelematic.director.http

import io.circe.syntax._
import com.advancedtelematic.libtuf.crypt.CanonicalJson._
import akka.http.scaladsl.model.StatusCodes
import com.advancedtelematic.director.data.Generators
import com.advancedtelematic.director.db.RepoNamespaceRepositorySupport
import com.advancedtelematic.director.util.{DirectorSpec, RepositorySpec, RouteResourceSpec}
import com.advancedtelematic.libtuf.data.ClientCodecs._
import com.advancedtelematic.libtuf.data.TufCodecs._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import com.advancedtelematic.director.data.GeneratorOps._
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.ErrorRepresentation
import com.advancedtelematic.libtuf.data.ClientDataType.{OfflineSnapshotRole, OfflineUpdatesRole, TufRole, ValidMetaPath}
import com.advancedtelematic.libtuf.data.TufDataType.SignedPayload
import com.advancedtelematic.libtuf_server.crypto.Sha256Digest
import eu.timepit.refined.api.Refined
import slick.jdbc.MySQLProfile.api._
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId._
import cats.syntax.show._

class OfflineUpdatesRoutesSpec extends DirectorSpec with RouteResourceSpec
  with RepoNamespaceRepositorySupport
  with AdminResources
  with RepositorySpec
  with Generators
  with DeviceResources {

  def forceRoleExpire[T](ns: Namespace)(implicit tufRole: TufRole[T]): Unit = {
    val sql = sql"update admin_roles set expires_at = '1970-01-01 00:00:00' where repo_id = (select repo_id from repo_namespaces where namespace = '#${ns.get}') and role = '#${tufRole.roleType.toString}'"
    db.run(sql.asUpdate).futureValue
  }

  testWithRepo("can add + retrieve an offline target") { implicit ns =>
    val (targetFilename, clientTarget) = GenTarget.generate

    Put(apiUri(s"admin/repo/offline-updates/emea/$targetFilename"), clientTarget).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    Get(apiUri("admin/repo/offline-updates/emea.json")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK

      val resp = responseAs[SignedPayload[OfflineUpdatesRole]]

      resp.signed.targets shouldBe Map(targetFilename -> clientTarget)
    }
  }

  testWithRepo("adding a new target keeps old targets") { implicit ns =>
    val (targetFilename0, clientTarget0) = GenTarget.generate

    Put(apiUri(s"admin/repo/offline-updates/emea/$targetFilename0"), clientTarget0).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    val (targetFilename1, clientTarget1) = GenTarget.generate

    Put(apiUri(s"admin/repo/offline-updates/emea/$targetFilename1"), clientTarget1).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    Get(apiUri("admin/repo/offline-updates/emea.json")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK

      val resp = responseAs[SignedPayload[OfflineUpdatesRole]]

      resp.signed.targets shouldBe Map(targetFilename0 -> clientTarget0, targetFilename1 -> clientTarget1)
    }
  }

  testWithRepo("404 if offline targets do not exist") { implicit ns =>
    Get(apiUri("admin/repo/offline-updates/emea.json")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.NotFound
      responseAs[ErrorRepresentation].code shouldBe ErrorCodes.MissingAdminRole
    }
  }

  testWithRepo("offline-snapshot.json is updated when targets change") { implicit ns =>
    val (targetFilename0, clientTarget0) = GenTarget.generate

    Put(apiUri(s"admin/repo/offline-updates/emea/$targetFilename0"), clientTarget0).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    val offlineUpdate = Get(apiUri("admin/repo/offline-updates/emea.json")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
      val resp = responseAs[SignedPayload[OfflineUpdatesRole]]
      resp.signed.targets shouldBe Map(targetFilename0 -> clientTarget0)

      resp
    }

    Get(apiUri("admin/repo/offline-snapshot.json")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
      val resp = responseAs[SignedPayload[OfflineSnapshotRole]]

      val canonical = offlineUpdate.asJson.canonical
      val checksum = Sha256Digest.digest(canonical.getBytes)

      val metaPath = Refined.unsafeApply[String, ValidMetaPath]("emea.json")

      val metaItem = resp.signed.meta(metaPath)
      metaItem.length shouldBe canonical.length
      metaItem.version shouldBe offlineUpdate.signed.version
      metaItem.hashes.head._2 shouldBe checksum.hash
    }
  }

  testWithRepo("deletes target") { implicit ns =>
    val (targetFilename0, clientTarget0) = GenTarget.generate

    Put(apiUri(s"admin/repo/offline-updates/emea/$targetFilename0"), clientTarget0).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    val (targetFilename1, clientTarget1) = GenTarget.generate

    Put(apiUri(s"admin/repo/offline-updates/emea/$targetFilename1"), clientTarget1).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    Delete(apiUri(s"admin/repo/offline-updates/emea/$targetFilename0")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    Get(apiUri("admin/repo/offline-updates/emea.json")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK

      val resp = responseAs[SignedPayload[OfflineUpdatesRole]]
      resp.signed.targets.get(targetFilename0) shouldNot be(defined)

      resp.signed.targets.get(targetFilename1) shouldBe defined
    }

    Get(apiUri("admin/repo/offline-snapshot.json")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
      val resp = responseAs[SignedPayload[OfflineSnapshotRole]]
      resp.signed.version shouldBe 3
    }
  }

  testWithRepo("returns 404 on DELETE an nonexistent target"){ implicit ns =>
    val (targetFilename0, clientTarget0) = GenTarget.generate

    Put(apiUri(s"admin/repo/offline-updates/emea/$targetFilename0"), clientTarget0).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    Delete(apiUri(s"admin/repo/offline-updates/emea/$targetFilename0")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    Delete(apiUri(s"admin/repo/offline-updates/emea/$targetFilename0")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.NotFound
    }
  }

  testWithRepo("device can get offline updates metadata") { implicit ns =>
    val deviceId = registerDeviceOk()

    val (targetFilename0, clientTarget0) = GenTarget.generate

    Put(apiUri(s"admin/repo/offline-updates/emea/$targetFilename0"), clientTarget0).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    Get(apiUri(s"device/${deviceId.show}/offline-updates/emea.json")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
      val signedPayload = responseAs[SignedPayload[OfflineUpdatesRole]].signed
      signedPayload.targets shouldNot be(empty)
    }

    Get(apiUri(s"device/${deviceId.show}/offline-snapshot.json")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
      val signedPayload = responseAs[SignedPayload[OfflineSnapshotRole]].signed
      val metaPath = Refined.unsafeApply[String, ValidMetaPath]("emea.json")
      signedPayload.meta.get(metaPath) should be(defined)
    }
  }

  testWithRepo("offline-targets/*.json is refreshed if expires") { implicit ns =>
    val (targetFilename, clientTarget) = GenTarget.generate

    Put(apiUri(s"admin/repo/offline-updates/emea/$targetFilename"), clientTarget).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    val expiresAt = Get(apiUri("admin/repo/offline-updates/emea.json")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK

      val resp = responseAs[SignedPayload[OfflineUpdatesRole]]

      resp.signed.version shouldBe 1
      resp.signed.expires
    }

    forceRoleExpire[OfflineUpdatesRole](ns)
    Thread.sleep(1000) // Needed because times are truncated to seconds in json

    Get(apiUri("admin/repo/offline-updates/emea.json")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
      val resp = responseAs[SignedPayload[OfflineUpdatesRole]]

      resp.signed.version shouldBe 2
      resp.signed.expires.isAfter(expiresAt) shouldBe true
    }
  }

  testWithRepo("offline-snapshot.json is refreshed if expires") { implicit ns =>
    val (targetFilename, clientTarget) = GenTarget.generate

    Put(apiUri(s"admin/repo/offline-updates/emea/$targetFilename"), clientTarget).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    val expiresAt = Get(apiUri("admin/repo/offline-snapshot.json")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
      val resp = responseAs[SignedPayload[OfflineSnapshotRole]]
      resp.signed.version shouldBe 1
      resp.signed.expires
    }

    forceRoleExpire[OfflineSnapshotRole](ns)
    Thread.sleep(1000) // Needed because times are truncated to seconds in json

    Get(apiUri("admin/repo/offline-snapshot.json")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
      val resp = responseAs[SignedPayload[OfflineSnapshotRole]]
      resp.signed.version shouldBe 2
      resp.signed.expires.isAfter(expiresAt) shouldBe true
    }
  }

  // This will cause problems if:
  //
  // 1. the client gets an offline-snapshot.json, which didn't expire
  // 2. gets emea.json, which expired
  // 3. server refreshes emea.json, which triggers an offline-snapshot.json refresh
  // 4. the json file obtained in 1. is now outdated and invalid, so the metadata validation will fail
  //
  // However, since these are offline updates only, we can control the order this metadata is downloaded,
  // and therefore this is not an issue in this implementation
  testWithRepo("offline-snapshot.json is refreshed when offline-targets-*.json expires") { implicit ns =>
    val (targetFilename, clientTarget) = GenTarget.generate

    Put(apiUri(s"admin/repo/offline-updates/emea/$targetFilename"), clientTarget).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    Get(apiUri("admin/repo/offline-snapshot.json")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK

      val resp = responseAs[SignedPayload[OfflineSnapshotRole]]

      resp.signed.version shouldBe 1
    }

    forceRoleExpire[OfflineUpdatesRole](ns)
    Thread.sleep(1000) // Needed because times are truncated to seconds in json

    Get(apiUri("admin/repo/offline-updates/emea.json")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
      val resp = responseAs[SignedPayload[OfflineUpdatesRole]]

      resp.signed.version shouldBe 2
    }

    Get(apiUri("admin/repo/offline-snapshot.json")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
      val resp = responseAs[SignedPayload[OfflineSnapshotRole]]

      resp.signed.version shouldBe 2
    }
  }
}
