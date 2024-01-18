package com.advancedtelematic.director.daemon

import akka.Done
import akka.http.scaladsl.model.StatusCodes
import com.advancedtelematic.director.http.{AdminResources, DeviceResources}
import com.advancedtelematic.director.util.{DirectorSpec, RepositorySpec, RouteResourceSpec}
import com.advancedtelematic.director.data.ClientDataType
import com.advancedtelematic.director.data.Codecs.*
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import com.advancedtelematic.libats.messaging_datatype.Messages.DeleteDeviceRequest
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport.*
import org.scalacheck.Gen
import com.advancedtelematic.director.data.GeneratorOps.GenSample

class DeleteDeviceRequestListenerSpec extends DirectorSpec
                            with RouteResourceSpec with AdminResources with RepositorySpec with DeviceResources {

  val listener = new DeleteDeviceRequestListener()

  testWithRepo("a device and it's ecus can be (marked) deleted") { implicit ns =>
    val dev = registerAdminDeviceOk()
    val hardwareId = dev.ecus.values.head.hardwareId

    Get(apiUri(s"admin/devices?primaryHardwareId=${hardwareId.value}")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
      val page = responseAs[PaginationResult[ClientDataType.Device]]
      page.total should equal(1)
      page.values.head.id shouldBe dev.deviceId
    }
    Get(apiUri(s"admin/devices/ecus")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
      val page = responseAs[PaginationResult[Map[DeviceId, Seq[ClientDataType.Ecu]]]]
      page.total should equal(1)
      page.values.head(dev.deviceId).head.hardwareId shouldBe hardwareId
      page.values.head(dev.deviceId).head.primary shouldBe true
    }

    listener(DeleteDeviceRequest(ns, dev.deviceId)).futureValue

    Get(apiUri(s"admin/devices?primaryHardwareId=${hardwareId.value}")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
      val page = responseAs[PaginationResult[ClientDataType.Device]]
      page.total should equal(0)
      page.values shouldBe Symbol("empty")
    }
    Get(apiUri(s"admin/devices/ecus")).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
      val page = responseAs[PaginationResult[Map[DeviceId, Seq[ClientDataType.Ecu]]]]
      page.total should equal(0)
    }
  }

  test("OTA-2445: do not fail when deleting non-existent device") {
    val msg = DeleteDeviceRequest(Gen.identifier.map(Namespace(_)).generate, Gen.uuid.map(DeviceId(_)).generate)
    listener(msg).futureValue shouldBe Done
  }
}
