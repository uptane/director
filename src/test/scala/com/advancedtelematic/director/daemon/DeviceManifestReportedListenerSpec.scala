package com.advancedtelematic.director.daemon

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKitBase
import cats.implicits.toShow
import com.advancedtelematic.director.data.Codecs.*
import com.advancedtelematic.director.data.GeneratorOps.*
import com.advancedtelematic.director.data.Generators.*
import com.advancedtelematic.director.data.Messages
import com.advancedtelematic.director.data.Messages.DeviceManifestReported
import com.advancedtelematic.director.db.DeviceManifestRepositorySupport
import com.advancedtelematic.director.util.DirectorSpec
import com.advancedtelematic.libats.data.DataType
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import com.advancedtelematic.libats.test.MysqlDatabaseSpec
import com.advancedtelematic.libtuf.data.TufDataType.SignedPayload
import com.typesafe.config.ConfigFactory
import io.circe.syntax.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.scalatest.OptionValues.*

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.{ExecutionContext, Future}

class DeviceManifestReportedListenerSpec
    extends DirectorSpec
    with TestKitBase
    with MysqlDatabaseSpec
    with DeviceManifestRepositorySupport {

  override implicit def system: ActorSystem = ActorSystem(this.getClass.getSimpleName)

  val defaultNs = DataType.Namespace(this.getClass.getName)

  implicit lazy val ec: scala.concurrent.ExecutionContextExecutor = ExecutionContext.global

  lazy val listener = new DeviceManifestReportedListener(ConfigFactory.load())

  private def runListener(msgs: Seq[DeviceManifestReported]): Future[Done] = {
    val cm = msgs.map { msg =>
      CommittableMessage(
        new ConsumerRecord[Array[Byte], DeviceManifestReported](
          "topic",
          0,
          0,
          msg.deviceId.show.getBytes,
          msg
        ),
        null
      )
    }

    Source
      .fromIterator(() => cm.iterator)
      .via(listener.processingFlow)
      .runWith(Sink.ignore)
  }

  private def runListener(msg: DeviceManifestReported): Future[Done] =
    runListener(List(msg))

  test("it saves manifest to database") {
    val manifest = GenDeviceManifest.generate
    val signedManifest = SignedPayload(Seq.empty, manifest.asJson, manifest.asJson)

    val msg =
      Messages.DeviceManifestReported(defaultNs, DeviceId.generate(), signedManifest, Instant.now())

    runListener(msg).futureValue

    val (saved, receivedAt) = deviceManifestRepository.findLatest(msg.deviceId).futureValue.value

    saved shouldBe msg.manifest.signed
    receivedAt.truncatedTo(ChronoUnit.SECONDS) shouldBe msg.receivedAt.truncatedTo(
      ChronoUnit.SECONDS
    )
  }

  test("it doesn't create new row if manifest did not change") {
    val manifest = GenDeviceManifest.generate
    val signedManifest = SignedPayload(Seq.empty, manifest.asJson, manifest.asJson)

    val msg =
      Messages.DeviceManifestReported(defaultNs, DeviceId.generate(), signedManifest, Instant.now())

    runListener(msg).futureValue
    runListener(msg.copy(receivedAt = Instant.now().plusSeconds(30))).futureValue

    val all = deviceManifestRepository.findAll(msg.deviceId).futureValue.values

    all should have size 1
    all.head._1 shouldBe msg.manifest.json
    all.head._2 shouldBe after(msg.receivedAt)
  }

  test("it saves new manifest if manifest changed") {
    val device = DeviceId.generate()
    val manifest = GenDeviceManifest.generate
    val signedManifest = SignedPayload(Seq.empty, manifest.asJson, manifest.asJson)
    val msg = Messages.DeviceManifestReported(defaultNs, device, signedManifest, Instant.now())
    runListener(msg).futureValue

    val manifest2 = GenDeviceManifest.generate
    val signedManifest2 = SignedPayload(Seq.empty, manifest2.asJson, manifest2.asJson)
    val msg2 = Messages.DeviceManifestReported(defaultNs, device, signedManifest2, Instant.now())
    runListener(msg2).futureValue

    val all = deviceManifestRepository.findAll(device).futureValue.values.map(_._1)

    all should have size 2

    all should contain(manifest.asJson)
    all should contain(manifest2.asJson)
  }

  test("it keeps only the latest 200 manifests in database") {
    val device = DeviceId.generate()
    val now = Instant.now().truncatedTo(ChronoUnit.SECONDS)
    val manifests = (1 to 250).map { i =>
      val manifest = GenDeviceManifest.generate
      val signedManifest = SignedPayload(Seq.empty, manifest.asJson, manifest.asJson)
      Messages.DeviceManifestReported(defaultNs, device, signedManifest, now.plusSeconds(i))
    }

    runListener(manifests).futureValue

    val savedManifests = deviceManifestRepository.findAll(device, 0L, 1000L).futureValue.values
    val lastManifests = manifests.map(_.manifest.json).takeRight(200)

    savedManifests should have size 200
    savedManifests.map(_._2).reverse should be(sorted)
    savedManifests.map(_._1) should contain theSameElementsAs lastManifests
  }

  test("it keeps only the latest 200 manifests per device in database") {
    val devices = List(DeviceId.generate(), DeviceId.generate())
    val now = Instant.now().truncatedTo(ChronoUnit.SECONDS)
    val manifests = devices.flatMap { device =>
      (1 to 250).map { i =>
        val manifest = GenDeviceManifest.generate
        val signedManifest = SignedPayload(Seq.empty, manifest.asJson, manifest.asJson)
        Messages.DeviceManifestReported(defaultNs, device, signedManifest, now.plusSeconds(i))
      }
    }

    runListener(manifests).futureValue

    devices.foreach { device =>
      val savedManifests = deviceManifestRepository.findAll(device, 0, 1000L).futureValue.values
      val deviceManifests =
        manifests.filter(_.deviceId == device).map(_.manifest.json).takeRight(200)

      savedManifests should have size 200
      savedManifests.map(_._2).reverse should be(sorted)
      savedManifests.map(_._1) should contain theSameElementsAs deviceManifests
    }
  }

  test("when sending same manifest twice, only latest version is saved if in same batch") {
    val device = DeviceId.generate()
    val manifest = GenDeviceManifest.generate
    val signedManifest = SignedPayload(Seq.empty, manifest.asJson, manifest.asJson)
    val firstTime = Instant.now().truncatedTo(ChronoUnit.SECONDS)
    val secondTime = firstTime.plusSeconds(30)

    val msg1 = DeviceManifestReported(defaultNs, device, signedManifest, firstTime)
    val msg2 = DeviceManifestReported(defaultNs, device, signedManifest, secondTime)

    runListener(List(msg2, msg1)).futureValue

    val saved = deviceManifestRepository.findAll(device).futureValue.values

    saved should have size 1
    saved.head._1 shouldBe manifest.asJson
    saved.head._2 shouldBe secondTime
  }

  test("when sending same manifest twice, only latest processed is saved") {
    val device = DeviceId.generate()
    val manifest = GenDeviceManifest.generate
    val signedManifest = SignedPayload(Seq.empty, manifest.asJson, manifest.asJson)
    val firstTime = Instant.now().truncatedTo(ChronoUnit.SECONDS)
    val secondTime = firstTime.plusSeconds(30)

    val msg1 = DeviceManifestReported(defaultNs, device, signedManifest, firstTime)
    val msg2 = DeviceManifestReported(defaultNs, device, signedManifest, secondTime)

    runListener(msg2).futureValue
    runListener(msg1).futureValue

    val saved = deviceManifestRepository.findAll(device).futureValue.values

    saved should have size 1
    saved.head._1 shouldBe manifest.asJson
    saved.head._2 shouldBe firstTime
  }


}
