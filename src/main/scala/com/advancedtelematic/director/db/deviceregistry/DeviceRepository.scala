/*
 * Copyright (c) 2017 ATS Advanced Telematic Systems GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.advancedtelematic.director.db.deviceregistry

import java.time.Instant
import java.time.temporal.ChronoUnit
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import com.advancedtelematic.libats.slick.db.SlickValidatedGeneric.validatedStringMapper
import com.advancedtelematic.director.http.deviceregistry.Errors
import com.advancedtelematic.director.deviceregistry.data.DataType.{
  DeletedDevice,
  DeviceT,
  HibernationStatus,
  SearchParams
}
import com.advancedtelematic.director.deviceregistry.data.Device.*
import com.advancedtelematic.director.deviceregistry.data.DeviceStatus.DeviceStatus
import com.advancedtelematic.director.deviceregistry.data.Group.GroupId
import com.advancedtelematic.director.deviceregistry.data.GroupType.GroupType
import com.advancedtelematic.director.deviceregistry.data.*
import DbOps.{
  deviceTableToSlickOrder,
  PaginationResultOps
}
import GroupInfoRepository.groupInfos
import GroupMemberRepository.groupMembers
import SlickMappings.*
import com.advancedtelematic.libats.slick.db.SlickAnyVal.*
import com.advancedtelematic.libats.slick.db.SlickExtensions.*
import com.advancedtelematic.libats.slick.db.SlickUUIDKey.*
import slick.jdbc.MySQLProfile.api.*

import scala.concurrent.ExecutionContext
import cats.syntax.option.*
import slick.jdbc.{PositionedParameters, SetParameter}
import slick.lifted.Rep

import java.sql.Timestamp
import scala.annotation.unused
import Schema.*

object DeviceRepository {

  val deletedDevices = TableQuery[DeletedDeviceTable]

  def create(ns: Namespace, device: DeviceT)(implicit ec: ExecutionContext): DBIO[DeviceId] = {
    val uuid = device.uuid.getOrElse(DeviceId.generate())

    val dbDevice = Device(
      ns,
      uuid,
      device.deviceName,
      device.deviceId,
      device.deviceType,
      createdAt = Instant.now(),
      hibernated = device.hibernated.getOrElse(false)
    )

    val dbIO = devices += dbDevice
    dbIO
      .handleIntegrityErrors(Errors.ConflictingDevice(device.deviceName.some, device.deviceId.some))
      .andThen(GroupMemberRepository.addDeviceToDynamicGroups(ns, dbDevice, Map.empty))
      .map(_ => uuid)
      .transactionally
  }

  def findUuidFromUniqueDeviceIdOrCreate(ns: Namespace, deviceId: DeviceOemId, devT: DeviceT)(
    implicit ec: ExecutionContext): DBIO[(Boolean, DeviceId)] =
    for {
      devs <- findByDeviceIdQuery(ns, deviceId).result
      (created, uuid) <- devs match {
        case Seq()  => create(ns, devT).map((true, _))
        case Seq(d) => DBIO.successful((false, d.uuid))
        case _ => DBIO.failed(Errors.ConflictingDevice(devT.deviceName.some, devT.deviceId.some))
      }
    } yield (created, uuid)

  def exists(ns: Namespace, uuid: DeviceId)(implicit ec: ExecutionContext): DBIO[Device] =
    devices
      .filter(d => d.namespace === ns && d.uuid === uuid)
      .result
      .headOption
      .flatMap(_.fold[DBIO[Device]](DBIO.failed(Errors.MissingDevice))(DBIO.successful))

  def filterExisting(ns: Namespace, deviceOemIds: Set[DeviceOemId]): DBIO[Seq[DeviceId]] =
    devices
      .filter(_.namespace === ns)
      .filter(_.deviceId.inSet(deviceOemIds))
      .map(_.uuid)
      .result

  def findByDeviceIdQuery(ns: Namespace, deviceId: DeviceOemId): Query[DeviceTable, Device, Seq] =
    devices.filter(d => d.namespace === ns && d.deviceId === deviceId)

  def setDevice(ns: Namespace, uuid: DeviceId, deviceName: DeviceName, notes: Option[String])(
    implicit ec: ExecutionContext): DBIO[Unit] =
    devices
      .filter(_.namespace === ns)
      .filter(_.uuid === uuid)
      .map(r => r.deviceName -> r.notes)
      .update(deviceName -> notes)
      .handleIntegrityErrors(Errors.ConflictingDevice(deviceName.some))
      .handleSingleUpdateError(Errors.MissingDevice)

  def updateDevice(ns: Namespace,
                   uuid: DeviceId,
                   deviceName: Option[DeviceName],
                   notes: Option[String])(implicit ec: ExecutionContext): DBIO[Unit] = {
    val findQ = devices
      .filter(_.uuid === uuid)
      .filter(_.namespace === ns)

    val updateQ = (deviceName, notes) match {
      case (Some(_name), Some(_notes)) =>
        findQ.map(r => r.deviceName -> r.notes).update(_name -> Option(_notes))
      case (Some(_name), None)  => findQ.map(r => r.deviceName).update(_name)
      case (None, Some(_notes)) => findQ.map(r => r.notes).update(Option(_notes))
      case (None, None)         => DBIO.successful(0)
    }

    updateQ
      .handleIntegrityErrors(Errors.ConflictingDevice(deviceName))
      .handleSingleUpdateError(Errors.MissingDevice)
  }

  def findByUuid(uuid: DeviceId)(implicit ec: ExecutionContext): DBIO[Device] =
    devices
      .filter(_.uuid === uuid)
      .result
      .headOption
      .flatMap(_.fold[DBIO[Device]](DBIO.failed(Errors.MissingDevice))(DBIO.successful))

  def findByUuids(ns: Namespace, ids: Seq[DeviceId]): Query[DeviceTable, Device, Seq] =
    devices.filter(d => (d.namespace === ns) && (d.uuid.inSet(ids)))

  def findByOemIds(ns: Namespace, oemIds: Seq[DeviceOemId]): DBIO[Seq[Device]] =
    devices.filter(d => (d.namespace === ns) && (d.deviceId.inSet(oemIds))).result

  def findByDeviceUuids(ns: Namespace, deviceIds: Seq[DeviceId]): DBIO[Seq[Device]] =
    findByUuids(ns, deviceIds).result

  def updateLastSeen(uuid: DeviceId, when: Instant)(
    implicit ec: ExecutionContext): DBIO[(Boolean, Namespace)] = {
    val sometime = Some(when)

    val dbIO = for {
      (ns, activatedAt) <- devices
        .filter(_.uuid === uuid)
        .map(x => (x.namespace, x.activatedAt))
        .result
        .failIfNotSingle(Errors.MissingDevice)
      _ <- devices
        .filter(_.uuid === uuid)
        .map(x => (x.lastSeen, x.activatedAt))
        .update((sometime, activatedAt.orElse(sometime)))
    } yield (activatedAt.isEmpty, ns)

    dbIO.transactionally
  }

  def delete(ns: Namespace, uuid: DeviceId)(implicit ec: ExecutionContext): DBIO[Unit] = {
    val dbIO = for {
      device <- exists(ns, uuid)
      _ <- EventJournal.archiveIndexedEvents(uuid)
      _ <- EventJournal.deleteEvents(uuid)
      _ <- GroupMemberRepository.removeDeviceFromAllGroups(uuid)
      _ <- PublicCredentialsRepository.delete(uuid)
      _ <- SystemInfoRepository.delete(uuid)
      _ <- TaggedDeviceRepository.delete(uuid)
      _ <- devices.filter(d => d.namespace === ns && d.uuid === uuid).delete
      _ <- deletedDevices += DeletedDevice(ns, device.uuid, device.deviceId)
    } yield ()

    dbIO.transactionally
  }

  def deviceNamespace(uuid: DeviceId)(implicit ec: ExecutionContext): DBIO[Namespace] =
    devices
      .filter(_.uuid === uuid)
      .map(_.namespace)
      .result
      .failIfNotSingle(Errors.MissingDevice)

  def countActivatedDevices(ns: Namespace, start: Instant, end: Instant): DBIO[Int] = {
    @unused
    implicit val setInstant: SetParameter[Instant] = (value: Instant, pos: PositionedParameters) =>
      pos.setTimestamp(Timestamp.from(value))

    // Using raw sql because SQL will it's own instant mapping for the comparisons, instead of javaInstantMapping
    val sql =
      sql"""
            select count(*) from #${devices.baseTableRow.tableName} WHERE
            namespace = ${ns.get} AND
            activated_at >= $start AND
            activated_at < $end
        """

    sql.as[Int].head
  }

  def setDeviceStatus(uuid: DeviceId, status: DeviceStatus)(
    implicit ec: ExecutionContext): DBIO[Unit] =
    devices
      .filter(_.uuid === uuid)
      .map(_.deviceStatus)
      .update(status)
      .handleSingleUpdateError(Errors.MissingDevice)

  // Returns the previous hibernation status
  def setHibernationStatus(ns: Namespace, id: DeviceId, status: HibernationStatus)(
    implicit ec: ExecutionContext): DBIO[HibernationStatus] =
    devices
      .filter(_.namespace === ns)
      .filter(_.uuid === id)
      .map(_.hibernated)
      .update(status)
      .flatMap {
        case c if c >= 1 =>
          DBIO.successful(!status)
        case c if c == 0 =>
          DBIO.successful(status)
      }

}
