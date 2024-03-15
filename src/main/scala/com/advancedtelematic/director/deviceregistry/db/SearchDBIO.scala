package com.advancedtelematic.director.deviceregistry.db

import com.advancedtelematic.director.deviceregistry.data.*
import com.advancedtelematic.director.deviceregistry.data.DataType.SearchParams
import com.advancedtelematic.director.deviceregistry.data.Group.GroupId
import com.advancedtelematic.director.deviceregistry.data.GroupType.GroupType
import com.advancedtelematic.director.deviceregistry.db.DbOps.{PaginationResultOps, deviceTableToSlickOrder}
import com.advancedtelematic.director.deviceregistry.db.GroupInfoRepository.groupInfos
import com.advancedtelematic.director.deviceregistry.db.GroupMemberRepository.groupMembers
import com.advancedtelematic.director.deviceregistry.db.Schema.*
import com.advancedtelematic.director.deviceregistry.db.SlickMappings.*
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import com.advancedtelematic.libats.slick.db.SlickExtensions.*
import com.advancedtelematic.libats.slick.db.SlickUUIDKey.*
import com.advancedtelematic.libats.slick.db.SlickValidatedGeneric.validatedStringMapper
import slick.jdbc.MySQLProfile.api.*
import slick.lifted.Rep

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.ExecutionContext

object SearchDBIO {

  private def devicesForExpressionQuery(ns: Namespace, expression: GroupExpression) = {
    val all = devices.filter(_.namespace === ns).map(_.uuid)
    GroupExpressionAST.compileToSlick(expression)(Schema.devices, TaggedDeviceRepository.taggedDevices)(all).distinct
  }

  def searchByExpression(ns: Namespace, expression: GroupExpression): DBIO[Seq[DeviceId]] =
    devicesForExpressionQuery(ns, expression).result

  def countDevicesForExpression(ns: Namespace, expression: GroupExpression): DBIO[Int] =
    devicesForExpressionQuery(ns, expression).length.result

  private def optionalFilter[T](o: Option[T])(
    fn: (DeviceTable, T) => Rep[Boolean]): DeviceTable => Rep[Boolean] =
    dt =>
      o match {
        case None => true.bind
        case Some(t) => fn(dt, t)
      }

  private def searchQuery(ns: Namespace,
                          nameContains: Option[String],
                          groupId: Option[GroupId],
                          notSeenSinceHours: Option[Int]) = {

    val groupFilter = optionalFilter(groupId) { (dt, gid) =>
      dt.uuid in groupMembers.filter(_.groupId === gid).map(_.deviceUuid)
    }

    val nameContainsFilter = optionalFilter(nameContains) { (dt, s) =>
      dt.deviceName.mappedTo[String].toLowerCase.like(s"%${s.toLowerCase}%")
    }

    val notSeenSinceFilter = optionalFilter(notSeenSinceHours) { (dt, h) =>
      dt.lastSeen.map(i => i < Instant.now.minus(h, ChronoUnit.HOURS)).getOrElse(true.bind)
    }

    devices
      .filter(_.namespace === ns)
      .filter(groupFilter)
      .filter(nameContainsFilter)
      .filter(notSeenSinceFilter)
  }

  private def runQueryFilteringByName(ns: Namespace,
                                      query: Query[DeviceTable, Device, Seq],
                                      nameContains: Option[String]) = {
    val deviceIdsByName = searchQuery(ns, nameContains, None, None).map(_.uuid)
    query.filter(_.uuid in deviceIdsByName)
  }

  private val groupedDevicesQuery
  : (Namespace, Option[GroupType]) => Query[DeviceTable, Device, Seq] = (ns, groupType) =>
    groupInfos
      .maybeFilter(_.groupType === groupType)
      .filter(_.namespace === ns)
      .join(groupMembers)
      .on(_.id === _.groupId)
      .join(devices)
      .on(_._2.deviceUuid === _.uuid)
      .map(_._2)
      .distinct

  def search(ns: Namespace, params: SearchParams)(
    implicit ec: ExecutionContext): DBIO[PaginationResult[Device]] = {
    val query = params match {

      case SearchParams(Some(oemId), _, _, None, None, None, _, _, _, _, _, _, _, _, _, _, _, _) =>
        DeviceRepository.findByDeviceIdQuery(ns, oemId)

      case SearchParams(
      None,
      Some(true),
      gt,
      None,
      nameContains,
      None,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _
      ) =>
        runQueryFilteringByName(ns, groupedDevicesQuery(ns, gt), nameContains)

      case SearchParams(
      None,
      Some(false),
      gt,
      None,
      nameContains,
      None,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _
      ) =>
        val ungroupedDevicesQuery =
          devices.filterNot(_.uuid.in(groupedDevicesQuery(ns, gt).map(_.uuid)))
        runQueryFilteringByName(ns, ungroupedDevicesQuery, nameContains)

      case SearchParams(
      None,
      _,
      _,
      gid,
      nameContains,
      notSeenSinceHours,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _
      ) =>
        searchQuery(ns, nameContains, gid, notSeenSinceHours)

      case _ => throw new IllegalArgumentException("Invalid parameter combination.")
    }

    val sortBy = params.sortBy.getOrElse(DeviceSortBy.Name)
    val sortDirection = params.sortDirection.getOrElse(SortDirection.Asc)

    val activatedAfterFilter = optionalFilter(params.activatedAfter) { (dt, from) =>
      dt.activatedAt.map(i => i >= from).getOrElse(false.bind)
    }

    val activatedBeforeFilter = optionalFilter(params.activatedBefore) { (dt, to) =>
      dt.activatedAt.map(i => i < to).getOrElse(false.bind)
    }

    val lastSeenStartFilter = optionalFilter(params.lastSeenStart) { (dt, lastSeen) =>
      dt.lastSeen.map(i => i > lastSeen).getOrElse(false.bind)
    }

    val lastSeenEndFilter = optionalFilter(params.lastSeenEnd) { (dt, lastSeen) =>
      dt.lastSeen.map(i => i < lastSeen).getOrElse(false.bind)
    }

    query
      .maybeFilter(_.deviceStatus === params.status)
      .maybeFilter(_.hibernated === params.hibernated)
      .maybeFilter(_.createdAt > params.createdAtStart)
      .maybeFilter(_.createdAt < params.createdAtEnd)
      .filter(activatedAfterFilter)
      .filter(activatedBeforeFilter)
      .filter(lastSeenStartFilter)
      .filter(lastSeenEndFilter)
      .sortBy(devices => devices.ordered(sortBy, sortDirection))
      .paginateResult(params.offset.orDefaultOffset, params.limit.orDefaultLimit)
  }
}
