package com.advancedtelematic.director.db.deviceregistry

import com.advancedtelematic.director.deviceregistry.data.*
import com.advancedtelematic.director.deviceregistry.data.DataType.DeletedDevice
import com.advancedtelematic.director.deviceregistry.data.Device.*
import com.advancedtelematic.director.deviceregistry.data.DeviceStatus.DeviceStatus
import SlickMappings.*
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import com.advancedtelematic.libats.slick.db.SlickAnyVal.*
import com.advancedtelematic.libats.slick.db.SlickExtensions.*
import com.advancedtelematic.libats.slick.db.SlickUUIDKey.*
import com.advancedtelematic.libats.slick.db.SlickValidatedGeneric.validatedStringMapper
import slick.jdbc.MySQLProfile.api.*

import java.time.Instant

object Schema {

  protected[db] implicit val DeviceStatusColumnType: BaseColumnType[DeviceStatus.Value] =
    MappedColumnType.base[DeviceStatus.Value, String](_.toString, DeviceStatus.withName)

  // scalastyle:off
  class DeviceTable(tag: Tag) extends Table[Device](tag, "Device") {
    def namespace = column[Namespace]("namespace")
    def id = column[DeviceId]("uuid")
    def deviceName = column[DeviceName]("device_name")
    def oemId = column[DeviceOemId]("device_id")
    def rawId = column[String]("device_id")
    def deviceType = column[DeviceType]("device_type")
    def lastSeen = column[Option[Instant]]("last_seen")(javaInstantMapping.optionType)
    def createdAt = column[Instant]("created_at")(javaInstantMapping)
    def activatedAt = column[Option[Instant]]("activated_at")(javaInstantMapping.optionType)
    def deviceStatus = column[DeviceStatus]("device_status")
    def notes = column[Option[String]]("notes")
    def hibernated = column[Boolean]("hibernated")

    def * =
      (
        namespace,
        id,
        deviceName,
        oemId,
        deviceType,
        lastSeen,
        createdAt,
        activatedAt,
        deviceStatus,
        notes,
        hibernated
      ).shaped <> ((Device.apply _).tupled, Device.unapply)

    def pk = primaryKey("uuid", id)
  }

  // scalastyle:on
  protected[db] val devices = TableQuery[DeviceTable]

  class DeletedDeviceTable(tag: Tag) extends Table[DeletedDevice](tag, "DeletedDevice") {
    def namespace = column[Namespace]("namespace")
    def uuid = column[DeviceId]("device_uuid")
    def deviceId = column[DeviceOemId]("device_id")

    def * =
      (namespace, uuid, deviceId).shaped <>
        ((DeletedDevice.apply _).tupled, DeletedDevice.unapply)

    def pk = primaryKey("pk_deleted_device", (namespace, uuid, deviceId))
  }

  protected[db] val deletedDevices = TableQuery[DeletedDeviceTable]

}
