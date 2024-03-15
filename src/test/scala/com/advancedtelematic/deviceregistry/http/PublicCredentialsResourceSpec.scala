/*
 * Copyright (c) 2017 ATS Advanced Telematic Systems GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.advancedtelematic.deviceregistry.http

import akka.http.scaladsl.model.StatusCodes.*
import com.advancedtelematic.deviceregistry.data.DataType.DeviceT
import com.advancedtelematic.deviceregistry.data.{CredentialsType, Device}
import com.advancedtelematic.deviceregistry.http.PublicCredentialsResource.FetchPublicCredentials
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import io.circe.generic.auto.*
import org.scalacheck.{Arbitrary, Gen}

class PublicCredentialsResourceSpec extends ResourcePropSpec {

  import Device.*
  import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport.*

  val genCredentialsType: Gen[CredentialsType.CredentialsType] =
    Gen.oneOf(CredentialsType.values.toSeq)

  implicit lazy val arbCredentialsType: Arbitrary[CredentialsType.CredentialsType] = Arbitrary(
    genCredentialsType
  )

  property("GET requests fails on non-existent device") {
    forAll { (uuid: DeviceId) =>
      fetchPublicCredentials(uuid) ~> route ~> check(status shouldBe NotFound)
    }
  }

  property("GET request after PUT yields same credentials") {
    forAll { (deviceId: DeviceOemId, creds: Array[Byte]) =>
      val uuid = updatePublicCredentialsOk(deviceId, creds)

      fetchPublicCredentialsOk(uuid) shouldBe creds
    }
  }

  property("PUT uses existing uuid if device exists") {
    forAll { (devId: DeviceOemId, mdevT: DeviceT, creds: Array[Byte]) =>
      val devT = mdevT.copy(deviceId = devId)
      val uuid: DeviceId = createDeviceOk(devT)
      uuid shouldBe updatePublicCredentialsOk(devId, creds)

      // updatePublicCredentials didn't change the device
      fetchDevice(uuid) ~> route ~> check {
        status shouldBe OK
        val dev = responseAs[Device]
        dev.deviceName shouldBe devT.deviceName
        dev.deviceId shouldBe devT.deviceId
        dev.deviceType shouldBe devT.deviceType
      }
    }
  }

  property("Latest PUT is the one that wins") {
    forAll { (deviceId: DeviceOemId, creds1: Array[Byte], creds2: Array[Byte]) =>
      val uuid = updatePublicCredentialsOk(deviceId, creds1)
      updatePublicCredentialsOk(deviceId, creds2)

      fetchPublicCredentialsOk(uuid) shouldBe creds2
    }
  }

  property("Type of credentials is set correctly") {
    forAll {
      (deviceId: DeviceOemId,
       mdevT: DeviceT,
       creds: String,
       cType: CredentialsType.CredentialsType) =>
        val devT =
          mdevT.copy(deviceId = deviceId, credentials = Some(creds), credentialsType = Some(cType))
        val uuid: DeviceId = createDeviceWithCredentials(devT) ~> route ~> check {
          status shouldBe OK
          responseAs[DeviceId]
        }

        fetchPublicCredentials(uuid) ~> route ~> check {
          status shouldBe OK
          val dev = responseAs[FetchPublicCredentials]

          dev.uuid shouldBe uuid
          dev.credentialsType shouldBe cType
          dev.credentials shouldBe creds
        }
    }
  }

}
