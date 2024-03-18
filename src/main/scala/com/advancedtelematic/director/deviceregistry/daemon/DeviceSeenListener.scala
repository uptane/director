/*
 * Copyright (c) 2017 ATS Advanced Telematic Systems GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.advancedtelematic.director.deviceregistry.daemon

import akka.Done
import com.advancedtelematic.director.db.deviceregistry.DeviceRepository
import com.advancedtelematic.director.deviceregistry.data.DeviceStatus
import com.advancedtelematic.libats.messaging.MessageBusPublisher
import com.advancedtelematic.libats.messaging.MsgOperation.MsgOperation
import com.advancedtelematic.libats.messaging_datatype.Messages.DeviceSeen
import com.advancedtelematic.director.deviceregistry.messages.DeviceActivated
import com.advancedtelematic.director.http.deviceregistry.Errors
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import slick.jdbc.MySQLProfile.api.*

class DeviceSeenListener(messageBus: MessageBusPublisher)(
  implicit db: Database,
  ec: ExecutionContext)
    extends MsgOperation[DeviceSeen] {

  val _logger = LoggerFactory.getLogger(this.getClass)

  override def apply(msg: DeviceSeen): Future[Done] =
    db.run(DeviceRepository.updateLastSeen(msg.uuid, msg.lastSeen))
      .flatMap { case (activated, ns) =>
        if (activated) {
          messageBus
            .publishSafe(DeviceActivated(ns, msg.uuid, msg.lastSeen))
            .flatMap { _ =>
              db.run(DeviceRepository.setDeviceStatus(msg.uuid, DeviceStatus.UpToDate))
            }
        } else {
          Future.successful(Done)
        }
      }
      .recover {
        case Errors.MissingDevice =>
          _logger.warn(s"Ignoring event for missing or deleted device: $msg")
        case ex =>
          _logger.warn(s"Could not process $msg", ex)
      }
      .map { _ =>
        Done
      }

}
