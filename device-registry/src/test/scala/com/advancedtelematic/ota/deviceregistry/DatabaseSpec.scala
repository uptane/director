package com.advancedtelematic.ota.deviceregistry

import com.advancedtelematic.libats.test.MysqlDatabaseSpec
import com.typesafe.config.ConfigFactory
import org.scalatest.Suite

trait DatabaseSpec extends MysqlDatabaseSpec {
  self: Suite =>

  override val testDbConfig = ConfigFactory.load().getConfig("ats.device-registry.database")

  override def dbPlaceHolders: Map[String, String] = super.dbPlaceHolders ++ Map("old-schema" -> s"${schemaName}_dev_reg")
}
