package com.advancedtelematic.director.daemon


import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.server.Directives
import com.advancedtelematic.director.{Settings, VersionInfo}
import com.advancedtelematic.libats.http.{BootApp, BootAppDatabaseConfig, BootAppDefaultConfig}
import com.advancedtelematic.libats.messaging.{BusListenerMetrics, MessageBus, MessageListenerSupport, MetricsBusMonitor}
import com.advancedtelematic.libats.messaging_datatype.Messages.{DeleteDeviceRequest, DeviceEventMessage, DeviceSeen, DeviceUpdateEvent, EcuReplacement}
import com.advancedtelematic.libats.slick.db.{BootMigrations, DatabaseSupport}
import com.advancedtelematic.libats.slick.monitoring.DbHealthResource
import com.advancedtelematic.libtuf_server.data.Messages.TufTargetAdded
import com.advancedtelematic.metrics.MetricsSupport
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import com.advancedtelematic.libats.http.VersionDirectives.*
import com.advancedtelematic.libats.messaging.metrics.MonitoredBusListenerSupport
import com.advancedtelematic.metrics.prometheus.PrometheusMetricsSupport
import com.advancedtelematic.deviceregistry.daemon.{DeviceEventListener, DeviceSeenListener, DeviceUpdateEventListener, EcuReplacementListener}
import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.security.Security
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class DirectorDaemonBoot(override val globalConfig: Config, override val dbConfig: Config,
                         override val metricRegistry: MetricRegistry)
                        (implicit override val system: ActorSystem) extends BootApp
  with Directives
  with Settings
  with VersionInfo
  with BootMigrations
  with DatabaseSupport
  with MetricsSupport
  with MonitoredBusListenerSupport
  with MessageListenerSupport
  with PrometheusMetricsSupport {

  implicit val _db: slick.jdbc.MySQLProfile.backend.Database = db

  import system.dispatcher

  lazy val messageBus = MessageBus.publisher(system, globalConfig)

  def bind(): Future[ServerBinding] = {
    startListener[TufTargetAdded](new TufTargetAddedListener, new MetricsBusMonitor(metricRegistry, "director-v2-tuf-target-added"))
    startListener[DeleteDeviceRequest](new DeleteDeviceRequestListener, new MetricsBusMonitor(metricRegistry, "director-v2-delete-device-request"))

    // TODO: No longer needed, we can update tables directly from director
    // Device Registry Listeners
    startMonitoredListener[DeviceSeen](new DeviceSeenListener(messageBus))
    startMonitoredListener[DeviceEventMessage](new DeviceEventListener)
    startMonitoredListener[DeviceUpdateEvent](new DeviceUpdateEventListener(messageBus))
    startMonitoredListener[EcuReplacement](new EcuReplacementListener)

    val routes = versionHeaders(version) {
      prometheusMetricsRoutes ~
        DbHealthResource(versionMap, healthMetrics = Seq(new BusListenerMetrics(metricRegistry))).route
    }

    Http().newServerAt(host, daemonPort).bindFlow(routes)
  }

}

object DaemonBoot extends BootAppDefaultConfig with BootAppDatabaseConfig with VersionInfo {
  Security.addProvider(new BouncyCastleProvider())

  def main(args: Array[String]): Unit = {
    val directorDaemonFut = new DirectorDaemonBoot(globalConfig, dbConfig, MetricsSupport.metricRegistry).bind()

    Await.result(directorDaemonFut, Duration.Inf)
  }
}
