package com.advancedtelematic.director.http

import akka.http.scaladsl.server.{Directives, Route}
import com.advancedtelematic.director.db.DirectorDbDebug
import com.advancedtelematic.libats.debug.DebugRoutes
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import slick.jdbc.MySQLProfile.api.*

import scala.concurrent.ExecutionContext

class DirectorDebugResource()(implicit val db: Database, val ec: ExecutionContext) {

  import com.advancedtelematic.libats.debug.DebugDatatype.*
  import com.advancedtelematic.libats.http.UUIDKeyAkka.*

  val debug = new DirectorDbDebug()

  val route: Route = DebugRoutes.routes {
    Directives.concat(
      DebugRoutes.buildNavigation(debug.namespace_resources, debug.device_resources),
      DebugRoutes.buildGroupRoutes(NamespacePath, debug.namespace_resources),
      DebugRoutes.buildGroupRoutes(DeviceId.Path, debug.device_resources)
    )
  }

}
