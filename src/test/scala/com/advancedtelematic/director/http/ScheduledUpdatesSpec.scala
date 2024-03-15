package com.advancedtelematic.director.http

import org.scalatest.OptionValues.*
import org.scalatest.LoneElement.*
import akka.http.scaladsl.model.StatusCodes
import com.advancedtelematic.director.util.{
  DirectorSpec,
  NamespacedTests,
  RepositorySpec,
  RouteResourceSpec
}
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import org.scalactic.source.Position
import cats.syntax.show.*
import com.advancedtelematic.director.data.ClientDataType.CreateScheduledUpdateRequest
import com.advancedtelematic.libats.data.{ErrorRepresentation, PaginationResult}
import com.advancedtelematic.director.data.Codecs.*
import com.advancedtelematic.director.data.DataType.{ScheduledUpdate, ScheduledUpdateId}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport.*
import ScheduledUpdateId.*
import com.advancedtelematic.director.data.AdminDataType.MultiTargetUpdate
import com.advancedtelematic.director.data.Generators.GenTargetUpdateRequest

import java.time.Instant
import com.advancedtelematic.director.data.GeneratorOps.*
import com.advancedtelematic.libtuf.data.TufDataType.HardwareIdentifier
import io.circe.Json

trait ScheduledUpdatesResources {
  self: DirectorSpec & RouteResourceSpec & NamespacedTests =>

  def createScheduledUpdateOk(deviceId: DeviceId, hardwareId: HardwareIdentifier)(
    implicit ns: Namespace,
    pos: Position): ScheduledUpdateId = {
    val mtu = MultiTargetUpdate(Map(hardwareId -> GenTargetUpdateRequest.generate))
    createScheduledUpdateOk(deviceId, mtu)
  }

  def createScheduledUpdateOk(deviceId: DeviceId, mtu: MultiTargetUpdate)(
    implicit ns: Namespace,
    pos: Position): ScheduledUpdateId = {
    val mtuId = Post(apiUri("multi_target_updates"), mtu).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.Created
      responseAs[UpdateId]
    }

    val req =
      CreateScheduledUpdateRequest(device = deviceId, updateId = mtuId, scheduledAt = Instant.now())

    Post(
      apiUri(s"admin/devices/${deviceId.show}/scheduled-updates"),
      req
    ).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.Created
      responseAs[ScheduledUpdateId]
    }
  }

  def listScheduledUpdatesOK(
    deviceId: DeviceId)(implicit ns: Namespace, pos: Position): PaginationResult[ScheduledUpdate] =
    Get(apiUri(s"admin/devices/${deviceId.show}/scheduled-updates")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
      responseAs[PaginationResult[ScheduledUpdate]]
    }

}

class ScheduledUpdatesSpec
    extends DirectorSpec
    with RouteResourceSpec
    with AdminResources
    with RepositorySpec
    with DeviceResources
    with ScheduledUpdatesResources {

  testWithRepo("creates and lists a scheduled update") { implicit ns =>
    val regDev = registerAdminDeviceOk()

    val id = createScheduledUpdateOk(regDev.deviceId, regDev.primary.hardwareId)

    val existing = listScheduledUpdatesOK(regDev.deviceId).values.loneElement

    existing.deviceId shouldBe regDev.deviceId
    existing.id shouldBe id
    existing.status shouldBe ScheduledUpdate.Status.Scheduled
  }

  testWithRepo("returns error if device already has a scheduled update") { implicit ns =>
    val regDev = registerAdminDeviceOk()

    val mtu = MultiTargetUpdate(Map(regDev.primary.hardwareId -> GenTargetUpdateRequest.generate))

    val mtuId = Post(apiUri("multi_target_updates"), mtu).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.Created
      responseAs[UpdateId]
    }

    val req = CreateScheduledUpdateRequest(
      device = regDev.deviceId,
      updateId = mtuId,
      scheduledAt = Instant.now()
    )

    Post(
      apiUri(s"admin/devices/${regDev.deviceId.show}/scheduled-updates"),
      req
    ).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.Created
    }

    Post(
      apiUri(s"admin/devices/${regDev.deviceId.show}/scheduled-updates"),
      req
    ).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.BadRequest
      val error = responseAs[ErrorRepresentation]
      error.code shouldBe ErrorCodes.UpdateScheduleError

      val causeCode = error.cause.value.hcursor.downN(0).keys.flatMap(_.headOption)
      causeCode should contain("scheduled_update_exists")
    }
  }

  testWithRepo("deletes scheduled update") { implicit ns =>
    val regDev = registerAdminDeviceOk()

    val id = createScheduledUpdateOk(regDev.deviceId, regDev.primary.hardwareId)

    Delete(
      apiUri(s"admin/devices/${regDev.deviceId.show}/scheduled-updates/${id.show}")
    ).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    val existing = listScheduledUpdatesOK(regDev.deviceId)

    existing.values.loneElement.status shouldBe ScheduledUpdate.Status.Cancelled
  }

  testWithRepo("returns an error if device does not have compatible ECUs cannot be updated") {
    implicit ns =>
      val regDev = registerAdminDeviceOk()
      val mtuId = createMtuOk()
      val req = CreateScheduledUpdateRequest(
        device = regDev.deviceId,
        updateId = mtuId,
        scheduledAt = Instant.now()
      )

      Post(
        apiUri(s"admin/devices/${regDev.deviceId.show}/scheduled-updates"),
        req
      ).namespaced ~> routes ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[ErrorRepresentation].code shouldBe ErrorCodes.UpdateScheduleError
      }
  }

}
