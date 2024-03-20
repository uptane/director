package com.advancedtelematic.director.deviceregistry.daemon

import akka.http.scaladsl.model.StatusCodes.{NotFound, OK}
import cats.syntax.show.*
import com.advancedtelematic.director.daemon.DeleteDeviceRequestListener
import com.advancedtelematic.director.deviceregistry.data.DeviceGenerators.*
import com.advancedtelematic.director.deviceregistry.data.GeneratorOps.GenSample
import com.advancedtelematic.director.http.deviceregistry.DeviceRequests
import com.advancedtelematic.director.util.{DirectorSpec, ResourceSpec}
import com.advancedtelematic.libats.messaging.test.MockMessageBus
import com.advancedtelematic.libats.messaging_datatype.Messages.DeleteDeviceRequest
import org.scalatest.OptionValues.*
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.time.{Millis, Seconds, Span}

class DeletedDevicePublisherSpec extends DirectorSpec with ResourceSpec with DeviceRequests {

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(15, Millis))

  val deleteDeviceHandler = new DeleteDeviceRequestListener()
  val subject = new DeletedDevicePublisher(msgPub)

  test("deleted devices are published") {
    import org.scalatest.time.SpanSugar.*

    val device = genDeviceT.generate
    val deviceId = createDeviceOk(device)

    fetchDevice(deviceId) ~> routes ~> check {
      status shouldBe OK
    }

    // delete the device from the DB, no message bus involved
    deleteDeviceHandler(DeleteDeviceRequest(defaultNs, deviceId)).futureValue

    fetchDevice(deviceId) ~> routes ~> check {
      status shouldBe NotFound
    }

    subject.run().futureValue

    eventually(timeout(5.seconds), interval(100.millis)) {
      val msg = msgPub.findReceived[DeleteDeviceRequest](deviceId.show)
      msg.value.uuid shouldBe deviceId
    }
  }

}
