/*
 * Copyright (c) 2017 ATS Advanced Telematic Systems GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.advancedtelematic.director.http.deviceregistry

import com.advancedtelematic.director.util.{DefaultPatience, ResourceSpec, RouteResourceSpec}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

trait DeviceRegistryRequests
    extends DeviceRequests
    with GroupRequests
    with PublicCredentialsRequests {
  self: DefaultPatience & ResourceSpec & RouteResourceSpec & Matchers =>
}

// TODO: Remove this, not needed?
trait ResourcePropSpec extends AnyFunSuite with ResourceSpec with ScalaCheckPropertyChecks with Matchers {

  implicit override val generatorDrivenConfig =
    PropertyCheckConfiguration(minSuccessful = 1, minSize = 3)

}
