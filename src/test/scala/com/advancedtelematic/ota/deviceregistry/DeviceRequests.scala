/*
 * Copyright (c) 2017 ATS Advanced Telematic Systems GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.advancedtelematic.ota.deviceregistry

import java.time.OffsetDateTime

import akka.http.scaladsl.model.{HttpRequest, StatusCodes, Uri}
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.server.Route
import cats.syntax.show._
import com.advancedtelematic.ota.deviceregistry.data.Group.GroupId
import com.advancedtelematic.ota.deviceregistry.data.{Device, DeviceT, PackageId, Uuid}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.Json

import scala.concurrent.ExecutionContext

/**
  * Generic test resource object
  * Used in property-based testing
  */
object Resource {
  def uri(pathSuffixes: String*): Uri = {
    val BasePath = Path("/api") / "v1"
    Uri.Empty.withPath(pathSuffixes.foldLeft(BasePath)(_ / _))
  }
}

/**
  * Testing Trait for building Device requests
  */
trait DeviceRequests { self: ResourceSpec =>

  import StatusCodes._
  import com.advancedtelematic.ota.deviceregistry.data.Device._

  val api = "devices"

  def fetchDevice(uuid: Uuid): HttpRequest =
    Get(Resource.uri(api, uuid.show))

  def listDevices(): HttpRequest =
    Get(Resource.uri(api))

  def searchDevice(regex: String, offset: Long = 0, limit: Long = 50): HttpRequest =
    Get(
      Resource
        .uri(api)
        .withQuery(Query("regex" -> regex, "offset" -> offset.toString, "limit" -> limit.toString))
    )

  def fetchByDeviceId(deviceId: Device.DeviceId): HttpRequest =
    Get(Resource.uri(api).withQuery(Query("deviceId" -> deviceId.show)))

  def fetchByGroupId(groupId: GroupId, offset: Long = 0, limit: Long = 50): HttpRequest =
    Get(
      Resource
        .uri(api)
        .withQuery(
          Query("groupId" -> groupId.show, "offset" -> offset.toString, "limit" -> limit.toString)
        )
    )

  def fetchUngrouped(offset: Long = 0, limit: Long = 50): HttpRequest =
    Get(
      Resource
        .uri(api)
        .withQuery(
          Query("ungrouped" -> "true", "offset" -> offset.toString, "limit" -> limit.toString)
        )
    )

  def updateDevice(uuid: Uuid, device: DeviceT)(implicit ec: ExecutionContext): HttpRequest =
    Put(Resource.uri(api, uuid.show), device)

  def createDevice(device: DeviceT)(implicit ec: ExecutionContext): HttpRequest =
    Post(Resource.uri(api), device)

  def createDeviceOk(device: DeviceT)(implicit ec: ExecutionContext): Uuid =
    createDevice(device) ~> route ~> check {
      status shouldBe Created
      responseAs[Uuid]
    }

  def deleteDevice(uuid: Uuid)(implicit ec: ExecutionContext): HttpRequest =
    Delete(Resource.uri(api, uuid.show))

  def fetchSystemInfo(uuid: Uuid): HttpRequest =
    Get(Resource.uri(api, uuid.show, "system_info"))

  def createSystemInfo(uuid: Uuid, json: Json)(implicit ec: ExecutionContext): HttpRequest =
    Post(Resource.uri(api, uuid.show, "system_info"), json)

  def updateSystemInfo(uuid: Uuid, json: Json)(implicit ec: ExecutionContext): HttpRequest =
    Put(Resource.uri(api, uuid.show, "system_info"), json)

  def fetchNetworkInfo(uuid: Uuid)(implicit ec: ExecutionContext): HttpRequest = {
    val uri = Resource.uri(api, uuid.show, "system_info", "network")
    Get(uri)
  }

  def listGroupsForDevice(device: Uuid)(implicit ec: ExecutionContext): HttpRequest =
    Get(Resource.uri(api, device.show, "groups"))

  def installSoftware(device: Uuid, packages: Set[PackageId]): HttpRequest =
    Put(Resource.uri("mydevice", device.show, "packages"), packages)

  def installSoftwareOk(device: Uuid, packages: Set[PackageId])(implicit route: Route): Unit =
    installSoftware(device, packages) ~> route ~> check {
      status shouldBe StatusCodes.NoContent
    }

  def listPackages(device: Uuid, regex: Option[String] = None)(implicit ec: ExecutionContext): HttpRequest =
    regex match {
      case Some(r) =>
        Get(Resource.uri("devices", device.show, "packages").withQuery(Query("regex" -> r)))
      case None => Get(Resource.uri("devices", device.show, "packages"))
    }

  def getStatsForPackage(pkg: PackageId)(implicit ec: ExecutionContext): HttpRequest =
    Get(Resource.uri("device_count", pkg.name, pkg.version))

  def getActiveDeviceCount(start: OffsetDateTime, end: OffsetDateTime): HttpRequest =
    Get(
      Resource.uri("active_device_count").withQuery(Query("start" -> start.show, "end" -> end.show))
    )

  def getInstalledForAllDevices(offset: Long = 0, limit: Long = 50): HttpRequest =
    Get(
      Resource
        .uri("device_packages")
        .withQuery(Query("offset" -> offset.toString, "limit" -> limit.toString))
    )

  def getAffected(pkgs: Set[PackageId]): HttpRequest =
    Post(Resource.uri("device_packages", "affected"), pkgs)

  def getPackageStats(name: PackageId.Name): HttpRequest =
    Get(Resource.uri("device_packages", name))

  def recordEvents(deviceUuid: Uuid, events: Json): HttpRequest =
    Post(Resource.uri(api, deviceUuid.show, "events"), events)

  def getEvents(deviceUuid: Uuid): HttpRequest =
    Get(Resource.uri(api, deviceUuid.show, "events"))
}
