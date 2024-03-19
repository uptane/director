/*
 * Copyright (c) 2017 ATS Advanced Telematic Systems GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.advancedtelematic.director.http.deviceregistry

import akka.http.scaladsl.model.StatusCodes.*
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest, Multipart}
import cats.syntax.show.*
import com.advancedtelematic.director.deviceregistry.data.Device.DeviceOemId
import com.advancedtelematic.director.deviceregistry.data.Group.GroupId
import com.advancedtelematic.director.deviceregistry.data.Group.GroupId.*
import com.advancedtelematic.director.deviceregistry.data.GroupSortBy.GroupSortBy
import com.advancedtelematic.director.deviceregistry.data.GroupType.GroupType
import com.advancedtelematic.director.deviceregistry.data.{GroupExpression, GroupName, GroupType}
import com.advancedtelematic.director.util.RouteResourceSpec
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport.*
import io.circe.Json
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

import scala.util.Random
import com.advancedtelematic.director.deviceregistry.data.GroupGenerators.*

// TODO: Matchers should not be explicit
trait GroupRequests {
  self: RouteResourceSpec & Matchers =>

  private val defaultExpression = GroupExpression.from("deviceid contains abcd").toOption.get
  protected val groupsApi = "device_groups"

  def listDevicesInGroup(groupId: GroupId,
                         offset: Option[Long] = None,
                         limit: Option[Long] = None): HttpRequest =
    (offset, limit) match {
      case (None, None) =>
        Get(Resource.uri("device_groups", groupId.show, "devices"))
      case _ =>
        Get(
          Resource
            .uri("device_groups", groupId.show, "devices")
            .withQuery(
              Query(
                "offset" -> offset.getOrElse(0).toString,
                "limit" -> limit.getOrElse(50).toString
              )
            )
        )
    }

  def listDevicesInGroupOk(groupId: GroupId, deviceIds: Seq[DeviceId]): Assertion =
    listDevicesInGroup(groupId) ~> routes ~> check {
      status shouldBe OK
      responseAs[PaginationResult[DeviceId]].values should contain theSameElementsAs deviceIds
    }

  def getGroupDetails(groupId: GroupId): HttpRequest =
    Get(Resource.uri(groupsApi, groupId.show))

  def countDevicesInGroup(groupId: GroupId): HttpRequest =
    Get(Resource.uri(groupsApi, groupId.show, "count"))

  def listGroups(sortBy: Option[GroupSortBy] = None,
                 limit: Option[Long] = None,
                 nameContains: Option[String] = None): HttpRequest = {
    val m = List("sortBy" -> sortBy, "limit" -> limit, "nameContains" -> nameContains).collect {
      case (k, Some(v)) => k -> v.toString
    }.toMap
    Get(Resource.uri(groupsApi).withQuery(Query(m)))
  }

  def deleteGroup(groupId: GroupId): HttpRequest =
    Delete(Resource.uri("device_groups", groupId.show))

  def createGroup(body: Json): HttpRequest =
    Post(Resource.uri(groupsApi), body)

  def createGroup(groupType: GroupType,
                  expression: Option[GroupExpression],
                  groupName: Option[GroupName] = None): HttpRequest = {
    val name = groupName.getOrElse(genGroupName().sample.get)
    val expr = groupType match {
      case GroupType.static  => None
      case GroupType.dynamic => expression.orElse(Some(defaultExpression))
    }
    Post(Resource.uri(groupsApi), CreateGroup(name, groupType, expr))
  }

  def importGroup(groupName: GroupName, oemIds: Seq[DeviceOemId]): HttpRequest = {
    val multipartForm = Multipart.FormData(
      Multipart.FormData.BodyPart.Strict(
        "deviceIds",
        HttpEntity(ContentTypes.`text/csv(UTF-8)`, oemIds.map(_.underlying).mkString("\n")),
        Map("filename" -> "vins.csv")
      )
    )
    Post(Resource.uri(groupsApi).withQuery(Query("groupName" -> groupName.value)), multipartForm)
  }

  def createStaticGroupOk(name: GroupName = genGroupName().sample.get): GroupId =
    createGroup(GroupType.static, None, Some(name)) ~> routes ~> check {
      status shouldBe Created
      responseAs[GroupId]
    }

  def createDynamicGroupOk(expression: GroupExpression = defaultExpression,
                           name: GroupName = genGroupName().sample.get): GroupId =
    createGroup(GroupType.dynamic, Some(expression), Some(name)) ~> routes ~> check {
      status shouldBe Created
      responseAs[GroupId]
    }

  def createGroupOk(name: GroupName = genGroupName().sample.get): GroupId =
    if (Random.nextBoolean()) createStaticGroupOk(name) else createDynamicGroupOk(name = name)

  def addDeviceToGroup(groupId: GroupId, deviceUuid: DeviceId): HttpRequest =
    Post(Resource.uri(groupsApi, groupId.show, "devices", deviceUuid.show))

  def addDeviceToGroupOk(groupId: GroupId, deviceUuid: DeviceId): Unit =
    addDeviceToGroup(groupId, deviceUuid) ~> routes ~> check {
      status shouldBe OK
    }

  def removeDeviceFromGroup(groupId: GroupId, deviceId: DeviceId): HttpRequest =
    Delete(Resource.uri(groupsApi, groupId.show, "devices", deviceId.show))

  def renameGroup(groupId: GroupId, newGroupName: GroupName): HttpRequest =
    Put(
      Resource
        .uri(groupsApi, groupId.show, "rename")
        .withQuery(Query("groupName" -> newGroupName.value))
    )

}
